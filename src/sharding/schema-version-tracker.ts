// ---------------------------------------------------------------------------
// Schema Version Tracker — per-shard schema version tracking.
// ---------------------------------------------------------------------------

import type { Pool } from "pg";
import { SCHEMA_SYNC_SCHEMA } from "./schema-sync-schema.js";
import { SchemaHashCalculator } from "./schema-hash-calculator.js";

export class SchemaVersionTracker {
    private readonly coordination: Pool;
    private readonly schemaHashCalculator: SchemaHashCalculator;
    private schemaReady?: Promise<void>;
    private async ensureSchema(): Promise<void> {
        if (!this.schemaReady) this.schemaReady = this.coordination.query(SCHEMA_SYNC_SCHEMA).then(()=>undefined).catch(()=>undefined);
        await this.schemaReady;
    }

    constructor(coordination: Pool, schemaHashCalculator?: SchemaHashCalculator) {
        this.coordination = coordination;
        this.schemaHashCalculator = schemaHashCalculator ?? new SchemaHashCalculator(coordination);
    }

    async recordVersion(shardId: string, version: string, checksum: string): Promise<void> {
        return this.recordMigration(shardId, version, checksum);
    }

    async recordMigration(shardId: string, version: string, checksum: string): Promise<void> {
        await this.coordination.query(
            `
            INSERT INTO shard_schema_versions
                (shard_id, migration_version, checksum, applied_at, is_consistent, last_verified_at, updated_at, created_at)
            VALUES ($1, $2, $3, NOW(), TRUE, NOW(), NOW(), NOW())
            ON CONFLICT (shard_id, migration_version)
            DO UPDATE SET
                applied_at = EXCLUDED.applied_at,
                checksum = EXCLUDED.checksum,
                is_consistent = TRUE,
                last_verified_at = NOW(),
                updated_at = NOW()
            `,
            [shardId, version, checksum],
        );
    }

    async getShardVersion(shardId: string): Promise<any | null> {
        const result = await this.coordination.query(
            `SELECT * FROM shard_schema_versions WHERE shard_id = $1 ORDER BY applied_at DESC LIMIT 1`,
            [shardId],
        );
        if (result.rows.length === 0) return null;
        const row = result.rows[0];
        return this.mapRow(row);
    }

    async getAllShardVersions(): Promise<any[]> {
        const result = await this.coordination.query(
            `SELECT * FROM shard_schema_versions ORDER BY shard_id, applied_at DESC`,
        );
        return result.rows.map((row) => this.mapRow(row));
    }

    async getAllVersions(): Promise<any[]> {
        return this.getAllShardVersions();
    }

    async getVersionsByMigration(version: string): Promise<any[]> {
        const result = await this.coordination.query(
            `SELECT * FROM shard_schema_versions WHERE migration_version = $1 ORDER BY shard_id`,
            [version],
        );
        return result.rows.map((row) => this.mapRow(row));
    }

    async getShardVersionsByVersion(version: string): Promise<any[]> {
        return this.getVersionsByMigration(version);
    }

    async getVersionsForMigration(version: string): Promise<any[]> {
        return this.getVersionsByMigration(version);
    }

    async markInconsistent(shardId: string, version?: string): Promise<void> {
        await this.coordination.query(
            `UPDATE shard_schema_versions
             SET is_consistent = FALSE, updated_at = NOW(), last_verified_at = NOW()
             WHERE shard_id = $1 ${version ? "AND migration_version = $2" : ""}`,
            version ? [shardId, version] : [shardId],
        );
    }

    async markConsistent(shardId: string, version?: string): Promise<void> {
        await this.coordination.query(
            `UPDATE shard_schema_versions
             SET is_consistent = TRUE, updated_at = NOW(), last_verified_at = NOW()
             WHERE shard_id = $1 ${version ? "AND migration_version = $2" : ""}`,
            version ? [shardId, version] : [shardId],
        );
    }

    async identifyMissingShards(shardIds: string[], version?: string): Promise<string[]> {
        const tracked = await this.getAllShardVersions();
        const trackedIds = new Set(tracked.map((s) => s.shardId));
        const relevant = version
            ? (await this.getVersionsByMigration(version)).map((s) => s.shardId)
            : trackedIds;
        const trackedRelevant = version ? new Set(relevant) : trackedIds;
        return shardIds.filter((id) => !trackedRelevant.has(id));
    }

    async getMissingShards(shardIds: string[], version?: string): Promise<string[]> {
        return this.identifyMissingShards(shardIds, version);
    }

    async generateConsistencyReport(
        shardIdsOrVersion?: string[] | string | { shardIds?: string[]; shardManager?: any; expectedVersion?: string; expectedChecksum?: string },
        expectedVersionOrChecksum?: string,
        expectedChecksum?: string,
    ): Promise<any> {
        return this.validateConsistency(shardIdsOrVersion, expectedVersionOrChecksum, expectedChecksum);
    }

    async validateConsistency(
        shardIdsOrVersion?: string[] | string | { shardIds?: string[]; shardManager?: any; expectedVersion?: string; expectedChecksum?: string },
        expectedVersionOrChecksum?: string,
        expectedChecksum?: string,
    ): Promise<any> {
        let shardIds: string[] | undefined;
        let expectedVersion = expectedVersionOrChecksum;
        let expectedCheck = expectedChecksum;

        if (Array.isArray(shardIdsOrVersion)) {
            shardIds = shardIdsOrVersion;
        } else if (typeof shardIdsOrVersion === "string") {
            if (!expectedVersion) expectedVersion = shardIdsOrVersion;
        } else if (shardIdsOrVersion && typeof shardIdsOrVersion === "object") {
            shardIds = shardIdsOrVersion.shardIds ?? shardIdsOrVersion.shardIds;
            if (shardIdsOrVersion.shardManager?.shardIds) {
                shardIds = Array.from(shardIdsOrVersion.shardManager.shardIds);
            }
            expectedVersion = expectedVersion ?? shardIdsOrVersion.expectedVersion;
            expectedCheck = expectedCheck ?? shardIdsOrVersion.expectedChecksum;
        }

        const tracked = await this.getAllShardVersions();
        const trackedMap = new Map(tracked.map((s) => [s.shardId, s]));

        const expectedV = expectedVersion ?? this.latestVersion(tracked);
        const expectedC =
            expectedCheck ??
            (tracked.find((s) => s.schemaVersion === expectedV)?.checksum ??
                tracked[0]?.checksum ??
                "");

        const shards = tracked;
        const inconsistent: any[] = [];
        for (const shard of tracked) {
            if (shard.schemaVersion !== expectedV || shard.checksum !== expectedC) {
                inconsistent.push(shard);
            }
        }

        let missing: string[] = [];
        if (shardIds) {
            const trackedIds = new Set(tracked.map((s) => s.shardId));
            missing = shardIds.filter((id) => !trackedIds.has(id));
        }

        return {
            isConsistent: inconsistent.length === 0 && missing.length === 0,
            expectedVersion: expectedV,
            expectedChecksum: expectedC,
            shards,
            inconsistentShards: inconsistent,
            missingShards: missing,
        };
    }

    private latestVersion(shards: any[]): string {
        if (!shards.length) return "";
        return shards.reduce((latest, s) => (s.schemaVersion >= latest ? s.schemaVersion : latest), "");
    }

    private mapRow(row: any): any {
        return {
            shardId: row.shard_id,
            schemaVersion: row.migration_version ?? row.version,
            version: row.migration_version ?? row.version,
            state: row.state,
            checksum: row.checksum,
            tables: {},
            appliedAt: row.applied_at ? new Date(row.applied_at) : new Date(),
            isConsistent: row.is_consistent ?? true,
            lastVerifiedAt: row.last_verified_at ? new Date(row.last_verified_at) : undefined,
        };
    }
}
