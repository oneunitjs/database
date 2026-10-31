import type {
    ShardStrategy,
    ShardRoutingContext,
    ShardStrategyName,
    WeightedShardConfig,
    ShardRange,
} from "../types.js";
import { ShardConfigError, ShardNotFoundError } from "./errors.js";

// ---------------------------------------------------------------------------
// Shared hashing utilities
// ---------------------------------------------------------------------------

/**
 * FNV-1a 32-bit hash. Deterministic across runs and platforms.
 * Returns an unsigned 32-bit integer.
 */
export function fnv1a32(input: string): number {
    let hash = 2166136261;
    for (let i = 0; i < input.length; i++) {
        hash ^= input.charCodeAt(i);
        hash = (hash * 16777619) >>> 0;
    }
    return hash >>> 0;
}

/**
 * Normalize a shard key to a canonical string form.
 */
export function normalizeShardKey(shardKey: string): string {
    return shardKey.trim().toLowerCase();
}

/**
 * Serialize a composite key deterministically, preventing collisions from
 * naive concatenation. Uses length-prefixed components separated by a
 * delimiter that cannot appear in the length prefix.
 */
function serializeCompositeKey(components: Record<string, string | number | boolean | null | undefined>): string {
    const parts: string[] = [];
    for (const [key, value] of Object.entries(components)) {
        if (value === undefined || value === null) {
            parts.push(`${key}=:null`);
            continue;
        }
        const strValue = String(value);
        parts.push(`${key}=:${strValue.length}:${strValue}`);
    }
    return parts.join("|");
}

// ---------------------------------------------------------------------------
// Strategy 1: Hash (existing, preserved)
// ---------------------------------------------------------------------------

export class HashShardStrategy implements ShardStrategy {
    readonly name = "hash" as const;
    readonly version = 1;

    validate(_config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Hash strategy requires at least one shard");
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { shardIds } = context;
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("No shards configured for hash routing");
        }
        const normalized = normalizeShardKey(shardKey);
        const hash = fnv1a32(normalized);
        return shardIds[hash % shardIds.length];
    }
}

// ---------------------------------------------------------------------------
// Strategy 2: Consistent Hashing
// ---------------------------------------------------------------------------

interface RingNode {
    shardId: string;
    position: number;
    virtualNodeIndex: number;
}

export class ConsistentHashShardStrategy implements ShardStrategy {
    readonly name = "consistent-hash" as const;
    readonly version = 1;

    private ring: RingNode[] = [];
    private shardIds: string[] = [];

    validate(_config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Consistent-hash strategy requires at least one shard");
        }
    }

    private buildRing(shardIds: string[], virtualNodes: number): void {
        this.shardIds = [...shardIds];
        const nodes: RingNode[] = [];

        const vNodes = Math.max(1, virtualNodes);

        for (const shardId of shardIds) {
            for (let i = 0; i < vNodes; i++) {
                const replicaKey = `${shardId}:${i}`;
                nodes.push({
                    shardId,
                    position: fnv1a32(replicaKey),
                    virtualNodeIndex: i,
                });
            }
        }

        if (nodes.length === 0) {
            this.ring = [];
            return;
        }

        // Sort by position for binary search
        nodes.sort((a, b) => a.position - b.position);
        this.ring = nodes;
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { shardIds, virtualNodes = 100 } = context;

        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("No shards configured for consistent-hash routing");
        }

        // Rebuild ring if membership changed
        if (this.shardIds.length !== shardIds.length || !this.shardIds.every((id, i) => id === shardIds[i])) {
            this.buildRing(shardIds, virtualNodes);
        }

        const keyPosition = fnv1a32(normalizeShardKey(shardKey));

        // Binary search for the first node with position >= keyPosition
        let lo = 0;
        let hi = this.ring.length;
        while (lo < hi) {
            const mid = (lo + hi) >>> 1;
            if (this.ring[mid].position < keyPosition) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Wrap around to the first node if we're past the end
        if (this.ring.length === 0) {
            throw new ShardConfigError("Consistent-hash ring is empty");
        }
        const node = this.ring[lo % this.ring.length];
        return node.shardId;
    }
}

// ---------------------------------------------------------------------------
// Strategy 3: Rendezvous Hashing
// ---------------------------------------------------------------------------

export class RendezvousShardStrategy implements ShardStrategy {
    readonly name = "rendezvous" as const;
    readonly version = 1;

    validate(_config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Rendezvous strategy requires at least one shard");
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { shardIds, hashSeed = "" } = context;

        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("No shards configured for rendezvous routing");
        }

        const normalizedKey = normalizeShardKey(shardKey);
        let bestShardId: string | undefined;
        let bestScore = -1;

        for (const shardId of shardIds) {
            const score = fnv1a32(`${hashSeed}:${normalizedKey}:${shardId}`);
            if (score > bestScore) {
                bestScore = score;
                bestShardId = shardId;
            }
        }

        if (!bestShardId) {
            throw new ShardConfigError("Rendezvous routing failed to select a shard");
        }

        return bestShardId;
    }
}

// ---------------------------------------------------------------------------
// Strategy 4: Range Sharding
// ---------------------------------------------------------------------------

export class RangeShardStrategy implements ShardStrategy {
    readonly name = "range" as const;
    readonly version = 1;

    private sortedRanges: ShardRange[] = [];

    validate(config: unknown, shardIds: string[]): void {
        const cfg = config as { ranges?: unknown };
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Range strategy requires at least one shard");
        }
        if (!cfg.ranges || !Array.isArray(cfg.ranges) || cfg.ranges.length === 0) {
            throw new ShardConfigError("Range strategy requires at least one range definition");
        }

        const seenShardIds = new Set<string>();
        for (const range of cfg.ranges as unknown[]) {
            const r = range as ShardRange;
            if (!r.shardId || typeof r.shardId !== "string") {
                throw new ShardConfigError("Range must specify a non-empty shardId");
            }
            if (seenShardIds.has(r.shardId)) {
                throw new ShardConfigError(`Duplicate shardId in ranges: ${r.shardId}`);
            }
            seenShardIds.add(r.shardId);

            const startInclusive = r.startInclusive ?? true;
            const endInclusive = r.endInclusive ?? true;

            if (r.start > r.end || (r.start === r.end && !startInclusive && !endInclusive)) {
                throw new ShardConfigError(
                    `Invalid range for shard ${r.shardId}: start must be <= end`,
                );
            }
        }

        // Check for overlapping ranges
        const sorted = [...cfg.ranges as ShardRange[]].sort((a, b) => {
            const cmp = String(a.start).localeCompare(String(b.start));
            return cmp !== 0 ? cmp : Number(a.end) - Number(b.end);
        });

        for (let i = 1; i < sorted.length; i++) {
            const prev = sorted[i - 1];
            const curr = sorted[i];

            const prevEndInclusive = prev.endInclusive ?? true;
            const currStartInclusive = curr.startInclusive ?? true;

            const prevEndValue = Number(prev.end);
            const currStartValue = Number(curr.start);

            if (currStartValue < prevEndValue ||
                (currStartValue === prevEndValue && prevEndInclusive && currStartInclusive)) {
                throw new ShardConfigError(
                    `Overlapping ranges: ${prev.shardId} and ${curr.shardId}`,
                );
            }
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { ranges } = context;

        if (!ranges || ranges.length === 0) {
            throw new ShardConfigError("No ranges configured for range routing");
        }

        const keyValue = Number(shardKey);
        if (!Number.isFinite(keyValue)) {
            throw new ShardNotFoundError(
                `Range routing requires a numeric shard key, got: ${shardKey}`,
            );
        }

        // Binary search for the range containing the key
        const sortedRanges = [...ranges].sort((a, b) => Number(a.start) - Number(b.start));

        for (const range of sortedRanges) {
            const startInclusive = range.startInclusive ?? true;
            const endInclusive = range.endInclusive ?? true;

            const startValue = Number(range.start);
            const endValue = Number(range.end);

            const atStart = keyValue === startValue;
            const atEnd = keyValue === endValue;

            const startOk = startInclusive ? keyValue >= startValue : keyValue > startValue;
            const endOk = endInclusive ? keyValue <= endValue : keyValue < endValue;

            if ((startOk || (atStart && startInclusive)) && (endOk || (atEnd && endInclusive))) {
                return range.shardId;
            }
        }

        throw new ShardNotFoundError(
            `No range contains shard key: ${shardKey}`,
        );
    }
}

// ---------------------------------------------------------------------------
// Strategy 5: Directory (existing, preserved with improvements)
// ---------------------------------------------------------------------------

export class DirectoryShardStrategy implements ShardStrategy {
    readonly name = "directory" as const;
    readonly version = 2;

    validate(_config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Directory strategy requires at least one shard");
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { directory, unknownKeyPolicy = "throw", defaultShardId } = context;

        if (!directory) {
            throw new ShardConfigError("Directory routing requires a directory mapping");
        }

        const normalized = normalizeShardKey(shardKey);
        const shardId = directory[normalized];

        if (!shardId) {
            if (unknownKeyPolicy === "default" && defaultShardId) {
                return defaultShardId;
            }
            throw new ShardNotFoundError(
                `No directory mapping for shard key: ${shardKey}`,
            );
        }

        return shardId;
    }
}

// ---------------------------------------------------------------------------
// Strategy 6: Tenant-Aware Sharding
// ---------------------------------------------------------------------------

export class TenantShardStrategy implements ShardStrategy {
    readonly name = "tenant" as const;
    readonly version = 1;

    validate(_config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Tenant strategy requires at least one shard");
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { tenants, defaultTenantShardId } = context;

        if (!tenants || Object.keys(tenants).length === 0) {
            throw new ShardConfigError("Tenant routing requires a tenant mapping");
        }

        const normalized = normalizeShardKey(shardKey);
        const shardId = tenants[normalized];

        if (!shardId) {
            if (defaultTenantShardId) {
                return defaultTenantShardId;
            }
            throw new ShardNotFoundError(
                `No tenant mapping for shard key: ${shardKey}`,
            );
        }

        return shardId;
    }
}

// ---------------------------------------------------------------------------
// Strategy 7: Composite-Key Sharding
// ---------------------------------------------------------------------------

export class CompositeShardStrategy implements ShardStrategy {
    readonly name = "composite" as const;
    readonly version = 1;

    private readonly innerStrategy: ShardStrategy;

    constructor() {
        this.innerStrategy = new HashShardStrategy();
    }

    validate(config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Composite strategy requires at least one shard");
        }
        const cfg = config as { hashStrategy?: string };
        const innerName = cfg.hashStrategy ?? "hash";
        if (!["hash", "consistent-hash", "rendezvous", "weighted"].includes(innerName)) {
            throw new ShardConfigError(
                `Composite strategy hashStrategy must be one of: hash, consistent-hash, rendezvous, weighted`,
            );
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { compositeKey, shardIds } = context;

        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("No shards configured for composite routing");
        }

        // Build a deterministic composite key string
        let serialized: string;
        if (compositeKey && Object.keys(compositeKey).length > 0) {
            serialized = serializeCompositeKey(compositeKey);
        } else {
            // Fall back to the raw shardKey
            serialized = normalizeShardKey(shardKey);
        }

        // Use the inner strategy to resolve
        const innerContext: ShardRoutingContext = {
            ...context,
            shardKey: serialized,
        };

        return this.innerStrategy.resolveShard(serialized, innerContext);
    }
}

// ---------------------------------------------------------------------------
// Strategy 8: Geographic Sharding
// ---------------------------------------------------------------------------

export class GeographicShardStrategy implements ShardStrategy {
    readonly name = "geographic" as const;
    readonly version = 1;

    validate(_config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Geographic strategy requires at least one shard");
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { regions, defaultRegion, unknownRegionPolicy = "throw" } = context;

        if (!regions || Object.keys(regions).length === 0) {
            throw new ShardConfigError("Geographic routing requires a region mapping");
        }

        const normalizedRegion = normalizeShardKey(shardKey);
        const eligibleShards = regions[normalizedRegion];

        if (!eligibleShards || eligibleShards.length === 0) {
            if (unknownRegionPolicy === "default" && defaultRegion) {
                const defaultShards = regions[normalizeShardKey(defaultRegion)];
                if (!defaultShards || defaultShards.length === 0) {
                    throw new ShardNotFoundError(
                        `No eligible shards for default region: ${defaultRegion}`,
                    );
                }
                // Deterministically pick from eligible shards using hash
                const hash = fnv1a32(normalizeShardKey(shardKey));
                return defaultShards[hash % defaultShards.length];
            }
            throw new ShardNotFoundError(
                `No eligible shards for region: ${shardKey}`,
            );
        }

        // Deterministically select from eligible shards for this region
        const hash = fnv1a32(`${normalizedRegion}:${shardKey}`);
        return eligibleShards[hash % eligibleShards.length];
    }
}

// ---------------------------------------------------------------------------
// Strategy 9: Weighted Sharding
// ---------------------------------------------------------------------------

export class WeightedShardStrategy implements ShardStrategy {
    readonly name = "weighted" as const;
    readonly version = 1;

    validate(config: unknown, shardIds: string[]): void {
        if (!shardIds || shardIds.length === 0) {
            throw new ShardConfigError("Weighted strategy requires at least one shard");
        }

        const cfg = config as { shards?: WeightedShardConfig[] };
        if (!cfg.shards || !Array.isArray(cfg.shards) || cfg.shards.length === 0) {
            throw new ShardConfigError("Weighted strategy requires weighted shard definitions");
        }

        const seenIds = new Set<string>();
        for (const ws of cfg.shards) {
            if (!ws.id || typeof ws.id !== "string") {
                throw new ShardConfigError("Weighted shard must have a non-empty id");
            }
            if (seenIds.has(ws.id)) {
                throw new ShardConfigError(`Duplicate weighted shard id: ${ws.id}`);
            }
            seenIds.add(ws.id);

            if (typeof ws.weight !== "number" || !Number.isFinite(ws.weight) || ws.weight <= 0) {
                throw new ShardConfigError(
                    `Weighted shard ${ws.id} must have a positive finite weight`,
                );
            }
        }
    }

    resolveShard(shardKey: string, context: ShardRoutingContext): string {
        const { weights } = context;

        if (!weights || Object.keys(weights).length === 0) {
            throw new ShardConfigError("No weights configured for weighted routing");
        }

        const shardIds = Object.keys(weights);
        if (shardIds.length === 0) {
            throw new ShardConfigError("No weighted shards available");
        }

        // Weighted highest-random-weight: score = hash^(1/weight)
        // For hash values in [0, 1), higher weight => higher score (since x^(1/w) > x for w > 1)
        const HASH_MODULUS = 4294967296; // 2^32
        const normalizedKey = normalizeShardKey(shardKey);
        let bestShardId: string | undefined;
        let bestScore = -1;

        for (const shardId of shardIds) {
            const weight = weights[shardId];
            if (!weight || weight <= 0) {
                continue;
            }
            const hash = fnv1a32(`${normalizedKey}:${shardId}`);
            const normalizedHash = hash / HASH_MODULUS;
            // score in (0, 1]: higher weight => higher score
            const score = Math.pow(normalizedHash > 0 ? normalizedHash : 1e-10, 1 / weight);
            if (score > bestScore) {
                bestScore = score;
                bestShardId = shardId;
            }
        }

        if (!bestShardId) {
            throw new ShardConfigError("Weighted routing failed to select a shard");
        }

        return bestShardId;
    }
}

// ---------------------------------------------------------------------------
// Strategy factory / registry
// ---------------------------------------------------------------------------

const STRATEGY_REGISTRY: Map<ShardStrategyName, () => ShardStrategy> = new Map();

STRATEGY_REGISTRY.set("hash", () => new HashShardStrategy());
STRATEGY_REGISTRY.set("consistent-hash", () => new ConsistentHashShardStrategy());
STRATEGY_REGISTRY.set("rendezvous", () => new RendezvousShardStrategy());
STRATEGY_REGISTRY.set("range", () => new RangeShardStrategy());
STRATEGY_REGISTRY.set("directory", () => new DirectoryShardStrategy());
STRATEGY_REGISTRY.set("tenant", () => new TenantShardStrategy());
STRATEGY_REGISTRY.set("composite", () => new CompositeShardStrategy());
STRATEGY_REGISTRY.set("geographic", () => new GeographicShardStrategy());
STRATEGY_REGISTRY.set("weighted", () => new WeightedShardStrategy());

export function createShardStrategy(name: ShardStrategyName): ShardStrategy {
    const factory = STRATEGY_REGISTRY.get(name);
    if (!factory) {
        throw new ShardConfigError(`Unknown shard strategy: ${String(name)}`);
    }
    return factory();
}

export function getRegisteredStrategyNames(): ShardStrategyName[] {
    return Array.from(STRATEGY_REGISTRY.keys());
}
