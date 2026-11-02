import { Pool } from "pg";
import type {
    ShardRoutingContext,
    ShardStrategy,
    ShardStrategyName,
    ShardRoutingDiagnostic,
    ShardRoutingDiagnostics,
} from "../types.js";
import {
    ShardConfigError,
    ShardNotFoundError,
    ShardingDisabledError,
} from "./errors.js";
import { createShardStrategy, registerShardStrategy } from "./strategies.js";
import { ShardManager } from "./shard-manager.js";

export interface ShardRouterOptions {
    manager: ShardManager;
    enabled: boolean;
    strategy?: ShardStrategyName;
    strategyInstance?: ShardStrategy;
    defaultShardId?: string;
    directory?: Record<string, string>;
    virtualNodes?: number;
    hashSeed?: string;
    ranges?: ShardRange[];
    tenants?: Record<string, string>;
    weights?: Record<string, number>;
    regions?: Record<string, string[]>;
    region?: string;
    compositeKey?: Record<string, string | number | boolean | null>;
    unknownKeyPolicy?: "throw" | "default";
    unknownRegionPolicy?: "throw" | "default";
}

export interface ShardRange {
    shardId: string;
    start: string | number;
    end: string | number;
    startInclusive?: boolean;
    endInclusive?: boolean;
}

export class ShardRouter {
    private readonly strategy: ShardStrategy | undefined;
    private readonly directory: Record<string, string> | undefined;
    private readonly defaultShardId: string | undefined;
    private readonly virtualNodes: number | undefined;
    private readonly hashSeed: string | undefined;
    private readonly ranges: ShardRange[] | undefined;
    private readonly tenants: Record<string, string> | undefined;
    private readonly weights: Record<string, number> | undefined;
    private readonly regions: Record<string, string[]> | undefined;
    private readonly region: string | undefined;
    private readonly compositeKey: Record<string, string | number | boolean | null> | undefined;
    private readonly unknownKeyPolicy: "throw" | "default" | undefined;
    private readonly unknownRegionPolicy: "throw" | "default" | undefined;

    // Routing diagnostics
    private readonly diagnostics: ShardRoutingDiagnostic[] = [];
    private readonly maxDiagnostics = 1000;
    private totalRoutes = 0;
    private byStrategy: Record<string, number> = {};
    private byShard: Record<string, number> = {};

    constructor(options: ShardRouterOptions) {
        this.manager = options.manager;
        this.enabled = options.enabled;
        this.defaultShardId = options.defaultShardId;
        this.directory = options.directory;
        this.virtualNodes = options.virtualNodes;
        this.hashSeed = options.hashSeed;
        this.ranges = options.ranges;
        this.tenants = options.tenants;
        this.weights = options.weights;
        this.regions = options.regions;
        this.region = options.region;
        this.compositeKey = options.compositeKey;
        this.unknownKeyPolicy = options.unknownKeyPolicy;
        this.unknownRegionPolicy = options.unknownRegionPolicy;

        if (options.enabled) {
            if (options.strategyInstance) {
                this.strategy = options.strategyInstance;
            } else if (options.strategy) {
                this.strategy = createShardStrategy(options.strategy);
            }
        }
    }

    private readonly manager: ShardManager;
    private readonly enabled: boolean;

    get isEnabled(): boolean {
        return this.enabled;
    }

    get strategyName(): ShardStrategyName | undefined {
        return this.strategy?.name;
    }

    get strategyVersion(): number | undefined {
        return this.strategy?.version;
    }

    // ---------------------------------------------------------------------------
    // Routing diagnostics
    // ---------------------------------------------------------------------------

    private recordDiagnostic(
        shardKey: string,
        selectedShardId: string,
        routingPath: string[],
        metadata: Record<string, unknown>,
    ): void {
        const diagnostic: ShardRoutingDiagnostic = {
            shardKey,
            selectedShardId,
            strategy: this.strategy?.name as ShardStrategyName,
            strategyVersion: this.strategy?.version ?? 0,
            routingPath,
            metadata,
            timestamp: new Date().toISOString(),
        };

        this.diagnostics.push(diagnostic);
        if (this.diagnostics.length > this.maxDiagnostics) {
            this.diagnostics.shift();
        }

        this.totalRoutes++;
        const s = this.strategy?.name ?? "unknown";
        this.byStrategy[s] = (this.byStrategy[s] ?? 0) + 1;
        this.byShard[selectedShardId] = (this.byShard[selectedShardId] ?? 0) + 1;
    }

    getDiagnostics(): ShardRoutingDiagnostics {
        return {
            recent: [...this.diagnostics],
            totalRoutes: this.totalRoutes,
            byStrategy: { ...this.byStrategy } as Record<ShardStrategyName, number>,
            byShard: { ...this.byShard },
            clear: () => {
                this.diagnostics.length = 0;
                this.totalRoutes = 0;
                this.byStrategy = {};
                this.byShard = {};
            },
        };
    }

    // ---------------------------------------------------------------------------
    // Routing
    // ---------------------------------------------------------------------------

    private buildContext(shardKey: string): ShardRoutingContext {
        return {
            shardKey,
            strategy: this.strategyName as ShardStrategyName,
            shardIds: this.manager.shardIds,
            directory: this.directory,
            virtualNodes: this.virtualNodes,
            hashSeed: this.hashSeed,
            ranges: this.ranges,
            tenants: this.tenants,
            weights: this.weights,
            regions: this.regions,
            region: this.region,
            compositeKey: this.compositeKey,
            unknownKeyPolicy: this.unknownKeyPolicy,
            defaultShardId: this.defaultShardId,
            unknownRegionPolicy: this.unknownRegionPolicy,
        };
    }

    resolveShardId(shardKey: string): string {
        if (!this.enabled) {
            throw new ShardingDisabledError();
        }
        if (!this.strategy) {
            throw new ShardConfigError(
                "Sharding is enabled but no routing strategy is configured",
            );
        }

        const context = this.buildContext(shardKey);
        const routingPath: string[] = [`strategy:${this.strategy.name}`];

        const shardId = this.strategy.resolveShard(shardKey, context);
        routingPath.push(`resolved:${shardId}`);

        if (!this.manager.hasShard(shardId)) {
            throw new ShardNotFoundError(shardId);
        }

        this.recordDiagnostic(shardKey, shardId, routingPath, {
            strategyName: this.strategy.name,
            strategyVersion: this.strategy.version,
            shardKeyHash: String(
                (context as { _hash?: number })._hash ?? 0,
            ),
        });

        return shardId;
    }

    resolveShard(shardKey: string): { id: string; pool: Pool } {
        const shardId = this.resolveShardId(shardKey);
        const shard = this.manager.getShard(shardId);
        if (!shard || !shard.pool) {
            throw new ShardNotFoundError(shardId);
        }
        return { id: shardId, pool: shard.pool };
    }

    resolveDefaultShard(): { id: string; pool: Pool } | undefined {
        if (!this.defaultShardId) {
            return undefined;
        }
        if (!this.manager.hasShard(this.defaultShardId)) {
            throw new ShardNotFoundError(this.defaultShardId);
        }
        const shard = this.manager.getShard(this.defaultShardId);
        if (!shard || !shard.pool) {
            throw new ShardNotFoundError(this.defaultShardId);
        }
        return { id: this.defaultShardId, pool: shard.pool };
    }

    validateStrategyConfig(config: unknown): void {
        if (!this.strategy) {
            return;
        }
        this.strategy.validate(config as never, this.manager.shardIds);
    }
}
