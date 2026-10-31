import { Pool } from "pg";
import type {
    ShardRoutingContext,
    ShardStrategy,
    ShardStrategyName,
} from "../types.js";
import {
    ShardConfigError,
    ShardNotFoundError,
    ShardingDisabledError,
} from "./errors.js";
import { createShardStrategy } from "./strategies.js";
import { ShardManager } from "./shard-manager.js";

export interface ShardRouterOptions {
    manager: ShardManager;
    enabled: boolean;
    strategy?: ShardStrategyName;
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

        if (options.enabled && options.strategy) {
            this.strategy = createShardStrategy(options.strategy);
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

        const shardId = this.strategy.resolveShard(shardKey, context);

        if (!this.manager.hasShard(shardId)) {
            throw new ShardNotFoundError(shardId);
        }

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
