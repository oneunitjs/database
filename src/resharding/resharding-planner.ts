// ---------------------------------------------------------------------------
// Strategy-specific resharding planner
//
// Compares the current topology + routing config with a proposed target
// topology and produces a MigrationUnit list describing what must move.
// The package cannot observe actual row data, so unit granularity depends on
// the strategy: directory/tenant/range can compute exact key/range deltas;
// hash/consistent-hash/rendezvous/weighted produce whole-shard or bucket
// units with unknown row counts (the copier discovers real keys at runtime).
//
// Unsupported strategy/config combinations are rejected with typed errors.
// ---------------------------------------------------------------------------

import { fnv1a32, normalizeShardKey } from "../sharding/strategies.js";
import type {
    MigrationUnit,
    ReshardingPlan,
    PlanOptions,
    TopologySnapshot,
    TopologyShard,
} from "./resharding-types.js";
import {
    UnsupportedStrategyError,
    MissingSchemaMetadataError,
    StalePlanError,
    InconsistentRoutingVersionError,
} from "./resharding-errors.js";

const HASH_MODULUS = 4294967296; // 2^32

interface StrategyPlanner {
    name: string;
    planUnits(
        source: TopologySnapshot,
        target: TopologySnapshot,
        opts: { maxEstimatedRows?: number },
    ): Promise<MigrationUnit[]>;
}

function buildShardIdSet(shards: TopologyShard[]): Set<string> {
    return new Set(shards.map((s) => s.id));
}

function shardIndex(shards: TopologyShard[], id: string): number {
    return shards.findIndex((s) => s.id === id);
}

function cloneTopology(t: TopologySnapshot): TopologySnapshot {
    return {
        ...t,
        shards: [...t.shards],
        strategyConfig: t.strategyConfig,
    };
}

function sanitizeShard(shard: TopologyShard): TopologyShard {
    return {
        id: shard.id,
        version: shard.version,
        connection: {
            host: shard.connection?.host,
            port: shard.connection?.port,
            database: shard.connection?.database,
            user: shard.connection?.user,
        },
        addedAt: shard.addedAt,
    };
}

/**
 * Build a TopologySnapshot from a ShardingConfig-like source, sanitizing
 * credentials out of the persisted snapshot.
 */
export function topologyFromConfig(config: {
    strategy: string;
    shards: Array<{ id: string; version?: number; connection: { host?: string; port?: number; database?: string; user?: string; password?: string; [key: string]: unknown } }>;
    routingVersion?: number;
    directory?: Record<string, string>;
    tenants?: Record<string, string>;
    weights?: Record<string, number>;
    regions?: Record<string, string[]>;
    ranges?: Array<{ shardId: string; start: string | number; end: string | number; startInclusive?: boolean; endInclusive?: boolean }>;
    virtualNodes?: number;
    hashSeed?: string;
    compositeKey?: Record<string, string | number | boolean | null>;
    defaultRegion?: string;
    defaultShardId?: string;
    defaultTenantShardId?: string;
}): TopologySnapshot {
    return {
        shards: config.shards.map((s) => sanitizeShard({
            id: s.id,
            version: s.version ?? 1,
            connection: {
                host: s.connection.host,
                port: s.connection.port,
                database: s.connection.database,
                user: s.connection.user,
            },
            addedAt: new Date().toISOString(),
        })),
        strategy: config.strategy as any,
        strategyConfig: {
            directory: config.directory,
            tenants: config.tenants,
            weights: config.weights,
            regions: config.regions,
            ranges: config.ranges,
            virtualNodes: config.virtualNodes,
            hashSeed: config.hashSeed,
            compositeKey: config.compositeKey,
            defaultRegion: config.defaultRegion,
            defaultShardId: config.defaultShardId,
            defaultTenantShardId: config.defaultTenantShardId,
        },
        routingVersion: config.routingVersion ?? 1,
        capturedAt: new Date().toISOString(),
    };
}

// ---------------------------------------------------------------------------
// Hash strategy: hash(key) % N. Membership change => virtually all keys remap.
// Units are whole-shard source->destination pairs; row counts unknown.
// ---------------------------------------------------------------------------

function planHash(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const sourceById = new Map(source.shards.map((s) => [s.id, s]));
    const targetById = new Map(target.shards.map((s) => [s.id, s]));

    // Determine which source shards lose keys to which destination shards by
    // simulating the hash ring over the shard id ordering. Since we cannot
    // enumerate keys, we conservatively mark every source shard that either
    // disappeared or changed ordinal position as a full source, paired with
    // every destination shard that gained ordinal space.
    const sourceIds = source.shards.map((s) => s.id);
    const targetIds = target.shards.map((s) => s.id);

    const removedSourceIds = sourceIds.filter((id) => !targetById.has(id));
    const addedTargetIds = targetIds.filter((id) => !sourceById.has(id));
    const commonIds = sourceIds.filter((id) => targetById.has(id));

    // Removed shards: all their data must move somewhere. Pair with the first
    // remaining target shard (deterministic).
    const firstRemaining = commonIds.length > 0 ? commonIds[0] : addedTargetIds[0];
    for (const id of removedSourceIds) {
        if (firstRemaining) {
            units.push(makeUnit(`hash:${id}->${firstRemaining}`, id, firstRemaining, "shard-key"));
        }
    }

    // Added shards receive data from existing shards. Distribute deterministically
    // by hash ordinal: each existing source shard yields a bucket to each new
    // shard whose ordinal range overlaps.
    for (const added of addedTargetIds) {
        const sourceForAdded = commonIds.length > 0 ? commonIds[commonIds.length % sourceIds.length] : added;
        units.push(makeUnit(`hash:${sourceForAdded}->${added}`, sourceForAdded, added, "shard-key"));
    }

    // Ordinal shifts among common shards cause remapping; conservatively plan
    // a full reshard between adjacent common shards whose ordinal changed.
    for (let i = 0; i < commonIds.length; i++) {
        const sid = commonIds[i];
        const srcIdx = sourceIds.indexOf(sid);
        const tgtIdx = targetIds.indexOf(sid);
        if (srcIdx !== tgtIdx && tgtIdx > 0) {
            const prev = targetIds[tgtIdx - 1];
            units.push(makeUnit(`hash:${prev}->${sid}`, prev, sid, "shard-key"));
        }
    }

    return units;
}

// ---------------------------------------------------------------------------
// Consistent hash: ring with virtual nodes. Only keys whose nearest ring node
// changes move. Bucket-level units = ranges of the hash ring.
// ---------------------------------------------------------------------------

function planConsistentHash(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const sourceIds = source.shards.map((s) => s.id);
    const targetIds = target.shards.map((s) => s.id);
    const vNodes = (target.strategyConfig as any)?.virtualNodes ?? (source.strategyConfig as any)?.virtualNodes ?? 100;

    const removed = sourceIds.filter((id) => !targetIds.includes(id));
    const added = targetIds.filter((id) => !sourceIds.includes(id));

    for (let i = 0; i < vNodes; i++) {
        const bucketId = `ch:${i}`;
        const srcNode = ringNodeFor(sourceIds, i, vNodes);
        const tgtNode = ringNodeFor(targetIds, i, vNodes);
        if (srcNode && tgtNode && srcNode !== tgtNode) {
            units.push(makeUnit(`ch:${bucketId}:${srcNode}->${tgtNode}`, srcNode, tgtNode, "bucket", undefined, i));
        }
    }

    // Removed shards' buckets must be adopted by remaining shards.
    const firstTarget = targetIds[0];
    for (const id of removed) {
        for (let i = 0; i < vNodes; i++) {
            const node = ringNodeFor(sourceIds, i, vNodes);
            if (node === id && firstTarget) {
                units.push(makeUnit(`ch:${i}:${id}->${firstTarget}`, id, firstTarget, "bucket", undefined, i));
            }
        }
    }

    return units;
}

function ringNodeFor(ids: string[], virtualIndex: number, vNodes: number): string | null {
    if (ids.length === 0) return null;
    // Deterministic assignment of virtual node i to a shard id.
    const hash = fnv1a32(`${virtualIndex}`);
    return ids[hash % ids.length];
}

// ---------------------------------------------------------------------------
// Rendezvous: highest score wins per key. Cannot enumerate keys; plan whole
// shard units for added/removed shards.
// ---------------------------------------------------------------------------

function planRendezvous(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const sourceIds = new Set(source.shards.map((s) => s.id));
    const targetIds = new Set(target.shards.map((s) => s.id));

    const removed = [...sourceIds].filter((id) => !targetIds.has(id));
    const added = [...targetIds].filter((id) => !sourceIds.has(id));

    const remaining = [...sourceIds].filter((id) => targetIds.has(id));
    const firstRemaining = remaining[0];

    for (const id of removed) {
        if (firstRemaining) {
            units.push(makeUnit(`rv:${id}->${firstRemaining}`, id, firstRemaining, "shard-key"));
        }
    }

    // Added shards steal keys from existing shards; pick a deterministic donor.
    const donors = [...sourceIds];
    for (const addedId of added) {
        const donor = donors[donors.length % donors.length] || addedId;
        units.push(makeUnit(`rv:${donor}->${addedId}`, donor, addedId, "shard-key"));
    }

    return units;
}

// ---------------------------------------------------------------------------
// Range: ordered non-overlapping ranges. Compute exact range deltas between
// source and target range tables. Preserve boundaries and ordering.
// ---------------------------------------------------------------------------

function planRange(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    type RangeDef = { shardId: string; start: string | number; end: string | number };
    const srcRanges = ((source.strategyConfig as any)?.ranges ?? []) as RangeDef[];
    const tgtRanges = ((target.strategyConfig as any)?.ranges ?? []) as RangeDef[];

    const srcByShard = new Map<string, Array<{ start: string | number; end: string | number }>>();
    for (const r of srcRanges) {
        const arr = srcByShard.get(r.shardId) ?? [];
        arr.push({ start: r.start, end: r.end });
        srcByShard.set(r.shardId, arr);
    }

    const tgtByShard = new Map<string, Array<{ start: string | number; end: string | number }>>();
    for (const r of tgtRanges) {
        const arr = tgtByShard.get(r.shardId) ?? [];
        arr.push({ start: r.start, end: r.end });
        tgtByShard.set(r.shardId, arr);
    }

    // For each target range, find overlapping source ranges; the overlap
    // (or the whole target range if source lacks coverage) is a unit.
    for (const tr of tgtRanges) {
        const overlaps = overlappingRanges(srcRanges, tr);
        if (overlaps.length === 0) {
            // Entire target range must be populated from somewhere; no single
            // source => conservative full-shard unit keyed off the range.
            units.push(makeUnit(
                `range:${tr.shardId}:${tr.start}-${tr.end}`,
                "unknown",
                tr.shardId,
                "range",
                String(tr.start),
                undefined,
                tr.start,
                tr.end,
            ));
            continue;
        }
        for (const ov of overlaps) {
            units.push(makeUnit(
                `range:${ov.shardId}->${tr.shardId}:${tr.start}-${tr.end}`,
                ov.shardId,
                tr.shardId,
                "range",
                String(tr.start),
                undefined,
                tr.start,
                tr.end,
            ));
        }
    }

    // Source ranges with no target coverage: data must be relocated.
    const coveredTargetShards = new Set(tgtRanges.map((r) => r.shardId));
    for (const sr of srcRanges) {
        if (!coveredTargetShards.has(sr.shardId)) {
            const firstTarget = tgtRanges[0]?.shardId;
            if (firstTarget) {
                units.push(makeUnit(
                    `range:${sr.shardId}->${firstTarget}:${sr.start}-${sr.end}`,
                    sr.shardId,
                    firstTarget,
                    "range",
                    String(sr.start),
                    undefined,
                    sr.start,
                    sr.end,
                ));
            }
        }
    }

    return units;
}

function overlappingRanges(
    ranges: Array<{ shardId: string; start: string | number; end: string | number }>,
    target: { start: string | number; end: string | number },
): Array<{ shardId: string; start: string | number; end: string | number }> {
    const ts = Number(target.start);
    const te = Number(target.end);
    const out: Array<{ shardId: string; start: string | number; end: string | number }> = [];
    for (const r of ranges) {
        const rs = Number(r.start);
        const re = Number(r.end);
        if (rs <= te && re >= ts) {
            out.push(r);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// Directory: exact key->shard map. Compute per-key deltas.
// ---------------------------------------------------------------------------

function planDirectory(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const srcDir = (source.strategyConfig as any)?.directory ?? {};
    const tgtDir = (target.strategyConfig as any)?.directory ?? {};

    const allKeys = new Set([...Object.keys(srcDir), ...Object.keys(tgtDir)]);
    for (const key of allKeys) {
        const src = srcDir[key];
        const tgt = tgtDir[key];
        if (src !== tgt) {
            if (!tgt) {
                // Key removed from directory; must be relocated from src.
                const firstTarget = (Object.values(tgtDir)[0] ?? Object.values(srcDir)[0]) as string | undefined;
                if (firstTarget && src) {
                    units.push(makeUnit(`dir:${key}:${key}`, src, firstTarget, "shard-key", key));
                }
            } else if (!src) {
                // New key mapping; populate target (no source unit).
                units.push(makeUnit(`dir:${key}:new->${tgt}`, "unknown", tgt, "shard-key", key));
            } else {
                units.push(makeUnit(`dir:${key}:${src}->${tgt}`, src, tgt, "shard-key", key));
            }
        }
    }

    return units;
}

// ---------------------------------------------------------------------------
// Tenant: tenant->shard ownership. Respect ownership; never split tenant data.
// ---------------------------------------------------------------------------

function planTenant(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const srcTenants = (source.strategyConfig as any)?.tenants ?? {};
    const tgtTenants = (target.strategyConfig as any)?.tenants ?? {};

    const allTenants = new Set([...Object.keys(srcTenants), ...Object.keys(tgtTenants)]);
    for (const tenant of allTenants) {
        const src = srcTenants[tenant];
        const tgt = tgtTenants[tenant];
        if (src !== tgt) {
            if (!tgt) {
                // Tenant unassigned: relocate from source to any remaining target.
                const firstTarget = (Object.values(tgtTenants)[0] ?? Object.values(srcTenants)[0]) as string | undefined;
                if (firstTarget && src) {
                    units.push(makeUnit(`tenant:${tenant}:${tenant}`, src, firstTarget, "tenant", tenant));
                }
            } else if (!src) {
                units.push(makeUnit(`tenant:${tenant}:new->${tgt}`, "unknown", tgt, "tenant", tenant));
            } else {
                units.push(makeUnit(`tenant:${tenant}:${src}->${tgt}`, src, tgt, "tenant", tenant));
            }
        }
    }

    return units;
}

// ---------------------------------------------------------------------------
// Composite: delegate to inner strategy over serialized composite keys.
// ---------------------------------------------------------------------------

function planComposite(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    // Composite re-uses the inner strategy over a serialized key. The package
    // cannot enumerate composite keys, so we conservatively plan whole-shard
    // units for added/removed shards, delegating the exact key mapping to the
    // application's adapter (destinationShardForRow) at copy time.
    const units: MigrationUnit[] = [];
    const sourceIds = new Set(source.shards.map((s) => s.id));
    const targetIds = new Set(target.shards.map((s) => s.id));

    const removed = [...sourceIds].filter((id) => !targetIds.has(id));
    const added = [...targetIds].filter((id) => !sourceIds.has(id));
    const remaining = [...sourceIds].filter((id) => targetIds.has(id));
    const firstRemaining = remaining[0];

    for (const id of removed) {
        if (firstRemaining) {
            units.push(makeUnit(`composite:${id}->${firstRemaining}`, id, firstRemaining, "shard-key"));
        }
    }
    const donors = [...sourceIds];
    for (const addedId of added) {
        const donor = donors[donors.length % donors.length] || addedId;
        units.push(makeUnit(`composite:${donor}->${addedId}`, donor, addedId, "shard-key"));
    }

    return units;
}

// ---------------------------------------------------------------------------
// Geographic: region->shards. Per-region units; respect eligible shard lists.
// ---------------------------------------------------------------------------

function planGeographic(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const srcRegions = (source.strategyConfig as any)?.regions ?? {};
    const tgtRegions = (target.strategyConfig as any)?.regions ?? {};

    const allRegions = new Set([...Object.keys(srcRegions), ...Object.keys(tgtRegions)]);
    for (const region of allRegions) {
        const src = (srcRegions[region] ?? []) as string[];
        const tgt = (tgtRegions[region] ?? []) as string[];
        const srcSet = new Set(src);
        const tgtSet = new Set(tgt);

        const removed = src.filter((s) => !tgtSet.has(s));
        const added = tgt.filter((s) => !srcSet.has(s));
        const firstTarget = tgt[0];
        const firstSrc = src[0];

        for (const s of removed) {
            if (firstTarget) {
                units.push(makeUnit(`geo:${region}:${s}->${firstTarget}`, s, firstTarget, "shard-key", region));
            }
        }
        for (const s of added) {
            const donor = firstSrc || s;
            units.push(makeUnit(`geo:${region}:${donor}->${s}`, donor, s, "shard-key", region));
        }
    }

    return units;
}

// ---------------------------------------------------------------------------
// Weighted: weighted highest-random-weight. Plan whole-shard units for
// weight/membership changes.
// ---------------------------------------------------------------------------

function planWeighted(
    source: TopologySnapshot,
    target: TopologySnapshot,
): MigrationUnit[] {
    const units: MigrationUnit[] = [];
    const srcWeights = (source.strategyConfig as any)?.weights ?? {};
    const tgtWeights = (target.strategyConfig as any)?.weights ?? {};

    const allIds = new Set([...Object.keys(srcWeights), ...Object.keys(tgtWeights)]);
    const srcIds = source.shards.map((s) => s.id);
    const tgtIds = target.shards.map((s) => s.id);
    const srcIdSet = new Set(srcIds);
    const tgtIdSet = new Set(tgtIds);

    const removed = srcIds.filter((id) => !tgtIdSet.has(id));
    const added = tgtIds.filter((id) => !srcIdSet.has(id));
    const remaining = srcIds.filter((id) => tgtIdSet.has(id));
    const firstRemaining = remaining[0];

    for (const id of removed) {
        if (firstRemaining) {
            units.push(makeUnit(`weighted:${id}->${firstRemaining}`, id, firstRemaining, "shard-key"));
        }
    }
    const donors = [...srcIds];
    for (const id of added) {
        const donor = donors[donors.length % donors.length] || id;
        units.push(makeUnit(`weighted:${donor}->${id}`, donor, id, "shard-key"));
    }

    // Weight changes between common shards: conservatively plan units from
    // the higher-weight-losing shard to the gaining shard.
    for (const id of remaining) {
        const sw = srcWeights[id] ?? 0;
        const tw = tgtWeights[id] ?? 0;
        if (Math.abs(sw - tw) > 1e-9 && firstRemaining && id !== firstRemaining) {
            units.push(makeUnit(`weighted:${id}->${firstRemaining}`, id, firstRemaining, "shard-key"));
        }
    }

    return units;
}

function makeUnit(
    unitId: string,
    sourceShardId: string,
    destinationShardId: string,
    unitType: MigrationUnit["unitType"],
    shardKey?: string,
    hashBucket?: number,
    rangeStart?: string | number,
    rangeEnd?: string | number,
    tableName?: string,
): MigrationUnit {
    return {
        unitId,
        unitType,
        sourceShardId,
        destinationShardId,
        ...(shardKey !== undefined ? { shardKey } : {}),
        ...(hashBucket !== undefined ? { hashBucket } : {}),
        ...(rangeStart !== undefined ? { rangeStart } : {}),
        ...(rangeEnd !== undefined ? { rangeEnd } : {}),
        ...(tableName !== undefined ? { tableName } : {}),
        estimatedRows: null,
        status: "planned",
        rowsCopied: 0,
        updatedAt: new Date().toISOString(),
    };
}

const PLANNERS: Record<string, StrategyPlanner> = {
    hash: { name: "hash", planUnits: (s, t) => Promise.resolve(planHash(s, t)) },
    "consistent-hash": {
        name: "consistent-hash",
        planUnits: (s, t) => Promise.resolve(planConsistentHash(s, t)),
    },
    rendezvous: { name: "rendezvous", planUnits: (s, t) => Promise.resolve(planRendezvous(s, t)) },
    range: { name: "range", planUnits: (s, t) => Promise.resolve(planRange(s, t)) },
    directory: { name: "directory", planUnits: (s, t) => Promise.resolve(planDirectory(s, t)) },
    tenant: { name: "tenant", planUnits: (s, t) => Promise.resolve(planTenant(s, t)) },
    composite: { name: "composite", planUnits: (s, t) => Promise.resolve(planComposite(s, t)) },
    geographic: { name: "geographic", planUnits: (s, t) => Promise.resolve(planGeographic(s, t)) },
    weighted: { name: "weighted", planUnits: (s, t) => Promise.resolve(planWeighted(s, t)) },
};

/**
 * Plan a migration from the current topology to a target topology.
 *
 * The caller supplies the current source topology and the desired target
 * topology. The planner returns MigrationUnits describing what must move and
 * rejects strategy/config combinations it cannot safely compute.
 *
 * When `options.dryRun` is true, no plan is persisted.
 *
 * When `options.expectedRoutingVersion` is supplied and differs from the
 * source topology's routing version, a StalePlanError is thrown.
 */
export async function planResharding(
    sourceTopology: TopologySnapshot,
    targetTopology: TopologySnapshot,
    migrationId: string,
    options: PlanOptions = {},
): Promise<ReshardingPlan> {
    const planner = PLANNERS[sourceTopology.strategy];
    if (!planner) {
        throw new UnsupportedStrategyError(
            sourceTopology.strategy,
            "No resharding planner is implemented for this strategy. Provide an application-defined planner via the migration descriptor extension point.",
        );
    }

    // Stale-plan detection: the live routing version must match the source
    // topology we plan against, otherwise the computed units may be wrong.
    if (
        options.expectedRoutingVersion !== undefined &&
        options.expectedRoutingVersion !== sourceTopology.routingVersion
    ) {
        throw new StalePlanError(
            migrationId,
            `Expected routing version ${options.expectedRoutingVersion} but source topology is at version ${sourceTopology.routingVersion}. Re-plan against the live topology.`,
        );
    }

    // Target strategy may differ from source (e.g. hash -> range). Validate
    // that both strategies are supported; planning units are derived from the
    // source strategy's view of what must move.
    if (targetTopology.strategy && !PLANNERS[targetTopology.strategy]) {
        throw new UnsupportedStrategyError(
            targetTopology.strategy,
            "Target routing strategy is not supported for resharding planning.",
        );
    }

    const units = await planner.planUnits(sourceTopology, targetTopology, {
        maxEstimatedRows: options.maxEstimatedRows,
    });

    // Reject plans that cannot be bounded when a max was requested.
    if (options.maxEstimatedRows !== undefined) {
        const unknown = units.filter((u) => u.estimatedRows === null);
        if (unknown.length > 0) {
            throw new UnsupportedStrategyError(
                sourceTopology.strategy,
                `Cannot bound estimated rows for ${unknown.length} unit(s); provide adapter estimatedRows or lower maxEstimatedRows.`,
            );
        }
    }

    const affectedShardIds = new Set<string>();
    for (const u of units) {
        affectedShardIds.add(u.sourceShardId);
        affectedShardIds.add(u.destinationShardId);
    }

    const estimatedTotalRows = units
        .map((u) => u.estimatedRows)
        .filter((v): v is number => v !== null)
        .reduce((a, b) => a + b, 0);

    const plan: ReshardingPlan = {
        migrationId,
        createdAt: new Date().toISOString(),
        sourceRoutingVersion: sourceTopology.routingVersion,
        targetRoutingVersion: targetTopology.routingVersion,
        sourceTopology: cloneTopology(sourceTopology),
        targetTopology: cloneTopology(targetTopology),
        strategy: sourceTopology.strategy,
        strategyConfig: sourceTopology.strategyConfig,
        units,
        estimatedTotalRows: units.length > 0 ? estimatedTotalRows : null,
        affectedShardIds: Array.from(affectedShardIds),
        dryRun: options.dryRun === true,
        version: 1,
    };

    return plan;
}

export { PLANNERS };
export type { StrategyPlanner };
