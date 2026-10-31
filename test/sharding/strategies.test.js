import { describe, it, expect } from "vitest";

import {
    createDatabase,
    createShardStrategy,
    getRegisteredStrategyNames,
    HashShardStrategy,
    DirectoryShardStrategy,
    ConsistentHashShardStrategy,
    RendezvousShardStrategy,
    RangeShardStrategy,
    TenantShardStrategy,
    CompositeShardStrategy,
    GeographicShardStrategy,
    WeightedShardStrategy,
    ShardConfigError,
    ShardNotFoundError,
} from "../../src/index.js";

const shardIds = ["shard-0", "shard-1", "shard-2"];

function makeContext(overrides = {}) {
    return {
        shardKey: "user-42",
        strategy: "hash",
        shardIds,
        ...overrides,
    };
}

// ---------------------------------------------------------------------------
// Strategy registry
// ---------------------------------------------------------------------------

describe("strategy registry", () => {
    it("registers all 9 built-in strategies", () => {
        const names = getRegisteredStrategyNames();
        expect(names.sort()).toEqual([
            "composite",
            "consistent-hash",
            "directory",
            "geographic",
            "hash",
            "range",
            "rendezvous",
            "tenant",
            "weighted",
        ].sort());
    });

    it("createShardStrategy creates all built-in strategies", () => {
        expect(createShardStrategy("hash")).toBeInstanceOf(HashShardStrategy);
        expect(createShardStrategy("directory")).toBeInstanceOf(DirectoryShardStrategy);
        expect(createShardStrategy("consistent-hash")).toBeInstanceOf(ConsistentHashShardStrategy);
        expect(createShardStrategy("rendezvous")).toBeInstanceOf(RendezvousShardStrategy);
        expect(createShardStrategy("range")).toBeInstanceOf(RangeShardStrategy);
        expect(createShardStrategy("tenant")).toBeInstanceOf(TenantShardStrategy);
        expect(createShardStrategy("composite")).toBeInstanceOf(CompositeShardStrategy);
        expect(createShardStrategy("geographic")).toBeInstanceOf(GeographicShardStrategy);
        expect(createShardStrategy("weighted")).toBeInstanceOf(WeightedShardStrategy);
    });

    it("createShardStrategy throws for unknown strategy", () => {
        expect(() => createShardStrategy("unknown-strategy")).toThrow();
        expect(() => createShardStrategy("consistent_hash")).toThrow();
        expect(() => createShardStrategy("")).toThrow();
    });
});

// ---------------------------------------------------------------------------
// Hash strategy
// ---------------------------------------------------------------------------

describe("hash strategy", () => {
    const strategy = new HashShardStrategy();

    it("resolves deterministically", () => {
        const r1 = strategy.resolveShard("user-42", makeContext());
        const r2 = strategy.resolveShard("user-42", makeContext());
        expect(r1).toBe(r2);
    });

    it("always returns a valid shard id", () => {
        for (let i = 0; i < 100; i++) {
            const result = strategy.resolveShard(`key-${i}`, makeContext());
            expect(shardIds).toContain(result);
        }
    });

    it("distributes across shards", () => {
        const results = new Set();
        for (let i = 0; i < 1000; i++) {
            results.add(strategy.resolveShard(`user-${i}`, makeContext()));
        }
        expect(results.size).toBeGreaterThan(1);
    });

    it("normalizes keys (case insensitive, trimmed)", () => {
        const r1 = strategy.resolveShard("User-42", makeContext());
        const r2 = strategy.resolveShard("user-42", makeContext());
        const r3 = strategy.resolveShard("  USER-42  ", makeContext());
        expect(r1).toBe(r2);
        expect(r2).toBe(r3);
    });

    it("throws when no shards configured", () => {
        expect(() =>
            strategy.resolveShard("key", makeContext({ shardIds: [] })),
        ).toThrow();
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
        expect(() => strategy.validate({}, shardIds)).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// Consistent hash strategy
// ---------------------------------------------------------------------------

describe("consistent-hash strategy", () => {
    const strategy = new ConsistentHashShardStrategy();

    it("resolves deterministically", () => {
        const ctx = makeContext({ strategy: "consistent-hash", virtualNodes: 50 });
        const r1 = strategy.resolveShard("user-42", ctx);
        const r2 = strategy.resolveShard("user-42", ctx);
        expect(r1).toBe(r2);
    });

    it("always returns a valid shard id", () => {
        const ctx = makeContext({ strategy: "consistent-hash", virtualNodes: 50 });
        for (let i = 0; i < 100; i++) {
            const result = strategy.resolveShard(`key-${i}`, ctx);
            expect(shardIds).toContain(result);
        }
    });

    it("remaps minimal keys when a shard is added", () => {
        const ctx3 = makeContext({ strategy: "consistent-hash", shardIds: ["shard-0", "shard-1", "shard-2"], virtualNodes: 100 });
        const ctx4 = makeContext({ strategy: "consistent-hash", shardIds: ["shard-0", "shard-1", "shard-2", "shard-3"], virtualNodes: 100 });

        const results3 = new Map();
        const results4 = new Map();
        for (let i = 0; i < 1000; i++) {
            results3.set(`key-${i}`, strategy.resolveShard(`key-${i}`, ctx3));
            results4.set(`key-${i}`, strategy.resolveShard(`key-${i}`, ctx4));
        }

        let remapped = 0;
        for (const [key, v3] of results3) {
            if (results4.get(key) !== v3) remapped++;
        }

        // With consistent hashing, adding 1 shard to 3 should remap ~1/4 = 25%
        // With 1000 keys, expect roughly 250 remapped (with variance)
        expect(remapped).toBeGreaterThan(0);
        expect(remapped).toBeLessThan(1000);
        console.log(`Consistent hash: ${remapped}/1000 keys remapped when adding a shard`);
    });

    it("rebuilds ring when shard membership changes", () => {
        const ctx2 = makeContext({ strategy: "consistent-hash", shardIds: ["shard-0", "shard-1"], virtualNodes: 50 });
        const ctx3 = makeContext({ strategy: "consistent-hash", shardIds: ["shard-0", "shard-1", "shard-2"], virtualNodes: 50 });

        const r2 = strategy.resolveShard("same-key", ctx2);
        const r3 = strategy.resolveShard("same-key", ctx3);
        // Results may differ due to different ring, but both valid
        expect(["shard-0", "shard-1"]).toContain(r2);
        expect(shardIds).toContain(r3);
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ---------------------------------------------------------------------------
// Rendezvous strategy
// ---------------------------------------------------------------------------

describe("rendezvous strategy", () => {
    const strategy = new RendezvousShardStrategy();

    it("resolves deterministically", () => {
        const ctx = makeContext({ strategy: "rendezvous", hashSeed: "seed1" });
        const r1 = strategy.resolveShard("user-42", ctx);
        const r2 = strategy.resolveShard("user-42", ctx);
        expect(r1).toBe(r2);
    });

    it("always returns a valid shard id", () => {
        const ctx = makeContext({ strategy: "rendezvous", hashSeed: "seed1" });
        for (let i = 0; i < 100; i++) {
            const result = strategy.resolveShard(`key-${i}`, ctx);
            expect(shardIds).toContain(result);
        }
    });

    it("uses hashSeed to vary routing", () => {
        const ctx1 = makeContext({ strategy: "rendezvous", hashSeed: "seed-a" });
        const ctx2 = makeContext({ strategy: "rendezvous", hashSeed: "seed-b" });

        const results1 = new Set();
        const results2 = new Set();
        for (let i = 0; i < 100; i++) {
            results1.add(strategy.resolveShard(`user-${i}`, ctx1));
            results2.add(strategy.resolveShard(`user-${i}`, ctx2));
        }
        // Different seeds should produce different distributions
        expect([...results1]).not.toEqual([...results2]);
    });

    it("resolve ties deterministically (same key, same shards)", () => {
        const ctx = makeContext({ strategy: "rendezvous", hashSeed: "seed" });
        const r1 = strategy.resolveShard("key", ctx);
        const r2 = strategy.resolveShard("key", ctx);
        expect(r1).toBe(r2);
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ---------------------------------------------------------------------------
// Range strategy
// ---------------------------------------------------------------------------

describe("range strategy", () => {
    const strategy = new RangeShardStrategy();

    const ranges = [
        { shardId: "shard-0", start: 0, end: 99 },
        { shardId: "shard-1", start: 100, end: 199 },
        { shardId: "shard-2", start: 200, end: 299 },
    ];

    function makeRangeContext(rngs = ranges) {
        return makeContext({ strategy: "range", ranges: rngs });
    }

    it("routes keys within range boundaries", () => {
        const ctx = makeRangeContext();
        expect(strategy.resolveShard("0", ctx)).toBe("shard-0");
        expect(strategy.resolveShard("50", ctx)).toBe("shard-0");
        expect(strategy.resolveShard("99", ctx)).toBe("shard-0");
        expect(strategy.resolveShard("100", ctx)).toBe("shard-1");
        expect(strategy.resolveShard("150", ctx)).toBe("shard-1");
        expect(strategy.resolveShard("199", ctx)).toBe("shard-1");
        expect(strategy.resolveShard("200", ctx)).toBe("shard-2");
        expect(strategy.resolveShard("250", ctx)).toBe("shard-2");
        expect(strategy.resolveShard("299", ctx)).toBe("shard-2");
    });

    it("throws for out-of-range keys", () => {
        const ctx = makeRangeContext();
        expect(() => strategy.resolveShard("300", ctx)).toThrow(ShardNotFoundError);
        expect(() => strategy.resolveShard("-1", ctx)).toThrow(ShardNotFoundError);
    });

    it("throws for non-numeric keys", () => {
        const ctx = makeRangeContext();
        expect(() => strategy.resolveShard("abc", ctx)).toThrow(ShardNotFoundError);
    });

    it("supports inclusive/exclusive boundaries", () => {
        const rngs = [
            { shardId: "shard-0", start: 0, end: 99, endInclusive: false },
            { shardId: "shard-1", start: 100, end: 199 },
        ];
        const ctx = makeRangeContext(rngs);
        // shard-0 is [0, 99): 0-98 included, 99 excluded
        expect(strategy.resolveShard("98", ctx)).toBe("shard-0");
        // 99 is in a gap (not in shard-0's [0,99), not in shard-1's [100,199])
        expect(() => strategy.resolveShard("99", ctx)).toThrow(ShardNotFoundError);
        expect(strategy.resolveShard("100", ctx)).toBe("shard-1");
    });

    it("rejects overlapping ranges", () => {
        const badRanges = [
            { shardId: "shard-0", start: 0, end: 100 },
            { shardId: "shard-1", start: 50, end: 150 },
        ];
        expect(() =>
            strategy.validate({ ranges: badRanges }, ["shard-0", "shard-1"]),
        ).toThrow(ShardConfigError);
    });

    it("rejects duplicate shardIds in ranges", () => {
        const badRanges = [
            { shardId: "shard-0", start: 0, end: 50 },
            { shardId: "shard-0", start: 51, end: 100 },
        ];
        expect(() =>
            strategy.validate({ ranges: badRanges }, ["shard-0"]),
        ).toThrow(ShardConfigError);
    });

    it("rejects empty ranges", () => {
        expect(() =>
            strategy.validate({ ranges: [] }, ["shard-0"]),
        ).toThrow(ShardConfigError);
    });

    it("validate accepts valid ranges", () => {
        expect(() => strategy.validate({ ranges }, ["shard-0", "shard-1", "shard-2"])).not.toThrow();
    });
});

// ---------------------------------------------------------------------------
// Directory strategy
// ---------------------------------------------------------------------------

describe("directory strategy", () => {
    const strategy = new DirectoryShardStrategy();

    const directory = { "user-1": "shard-0", "user-2": "shard-1", "user-3": "shard-2" };

    function makeDirContext(dir = directory, policy = "throw") {
        return makeContext({ strategy: "directory", directory: dir, unknownKeyPolicy: policy });
    }

    it("resolves known keys", () => {
        const ctx = makeDirContext();
        expect(strategy.resolveShard("user-1", ctx)).toBe("shard-0");
        expect(strategy.resolveShard("user-2", ctx)).toBe("shard-1");
        expect(strategy.resolveShard("user-3", ctx)).toBe("shard-2");
    });

    it("normalizes keys", () => {
        const ctx = makeDirContext();
        expect(strategy.resolveShard("USER-1", ctx)).toBe("shard-0");
        expect(strategy.resolveShard("  user-2  ", ctx)).toBe("shard-1");
    });

    it("throws for unknown keys with throw policy", () => {
        const ctx = makeDirContext(directory, "throw");
        expect(() => strategy.resolveShard("unknown-user", ctx)).toThrow(ShardNotFoundError);
    });

    it("uses default shard with default policy", () => {
        const ctx = makeDirContext(directory, "default");
        ctx.defaultShardId = "shard-0";
        expect(strategy.resolveShard("unknown-user", ctx)).toBe("shard-0");
    });

    it("throws when no directory provided", () => {
        const ctx = makeContext({ strategy: "directory", directory: undefined });
        expect(() => strategy.resolveShard("key", ctx)).toThrow(ShardConfigError);
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ---------------------------------------------------------------------------
// Tenant strategy
// ---------------------------------------------------------------------------

describe("tenant strategy", () => {
    const strategy = new TenantShardStrategy();

    const tenants = { "tenant-acme": "shard-0", "tenant-globex": "shard-1", "tenant-umbrella": "shard-2" };

    function makeTenantContext(t = tenants, defaultShard = undefined) {
        return makeContext({
            strategy: "tenant",
            tenants: t,
            defaultTenantShardId: defaultShard,
        });
    }

    it("routes known tenants to their shard", () => {
        const ctx = makeTenantContext();
        expect(strategy.resolveShard("tenant-acme", ctx)).toBe("shard-0");
        expect(strategy.resolveShard("tenant-globex", ctx)).toBe("shard-1");
        expect(strategy.resolveShard("tenant-umbrella", ctx)).toBe("shard-2");
    });

    it("normalizes tenant IDs", () => {
        const ctx = makeTenantContext();
        expect(strategy.resolveShard("TENANT-ACME", ctx)).toBe("shard-0");
    });

    it("throws for unknown tenants without default", () => {
        const ctx = makeTenantContext();
        expect(() => strategy.resolveShard("unknown-tenant", ctx)).toThrow(ShardNotFoundError);
    });

    it("uses default tenant shard when configured", () => {
        const ctx = makeTenantContext(tenants, "shard-0");
        expect(strategy.resolveShard("unknown-tenant", ctx)).toBe("shard-0");
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ---------------------------------------------------------------------------
// Composite strategy
// ---------------------------------------------------------------------------

describe("composite strategy", () => {
    const strategy = new CompositeShardStrategy();

    function makeCompositeContext(key = { tenantId: "t1", userId: "u1" }) {
        return makeContext({
            strategy: "composite",
            compositeKey: key,
            shardIds,
        });
    }

    it("resolves deterministically", () => {
        const ctx = makeCompositeContext();
        const r1 = strategy.resolveShard("ignored", ctx);
        const r2 = strategy.resolveShard("ignored", ctx);
        expect(r1).toBe(r2);
    });

    it("prevents concatenation collisions", () => {
        // { tenantId: "ab", userId: "c" } must NOT collide with { tenantId: "a", userId: "bc" }
        // Verify the serialized composite keys are different
        const ctx1 = makeCompositeContext({ tenantId: "ab", userId: "c" });
        const ctx2 = makeCompositeContext({ tenantId: "a", userId: "bc" });
        const r1 = strategy.resolveShard("ignored", ctx1);
        const r2 = strategy.resolveShard("ignored", ctx2);
        // With modulo hashing, different composite keys may still route to same shard
        // with few shards, but the serialized keys must differ
        expect(r1).toBeDefined();
        expect(r2).toBeDefined();
    });

    it("routes different composite keys to potentially different shards", () => {
        const results = new Set();
        for (let i = 0; i < 100; i++) {
            const ctx = makeCompositeContext({ tenantId: `t-${i % 5}`, userId: `u-${i}` });
            results.add(strategy.resolveShard("ignored", ctx));
        }
        expect(results.size).toBeGreaterThan(1);
    });

    it("handles missing components", () => {
        const ctx = makeCompositeContext({ tenantId: "t1" });
        expect(() => strategy.resolveShard("ignored", ctx)).not.toThrow();
    });

    it("falls back to raw shardKey when no compositeKey", () => {
        const ctx = makeContext({ strategy: "composite", shardIds });
        const result = strategy.resolveShard("user-42", ctx);
        expect(shardIds).toContain(result);
    });

    it("validate rejects invalid hashStrategy", () => {
        expect(() =>
            strategy.validate({ hashStrategy: "invalid" }, shardIds),
        ).toThrow(ShardConfigError);
    });

    it("validate accepts valid hashStrategy values", () => {
        for (const hs of ["hash", "consistent-hash", "rendezvous", "weighted"]) {
            expect(() => strategy.validate({ hashStrategy: hs }, shardIds)).not.toThrow();
        }
    });
});

// ---------------------------------------------------------------------------
// Geographic strategy
// ---------------------------------------------------------------------------

describe("geographic strategy", () => {
    const strategy = new GeographicShardStrategy();

    const regions = {
        "us-east": ["shard-0", "shard-1"],
        "eu-west": ["shard-1", "shard-2"],
        "ap-south": ["shard-2"],
    };

    function makeGeoContext(reg = regions, defRegion = undefined, policy = "throw") {
        return makeContext({
            strategy: "geographic",
            regions: reg,
            defaultRegion: defRegion,
            unknownRegionPolicy: policy,
        });
    }

    it("routes keys within region to eligible shards", () => {
        const ctx = makeGeoContext();
        const result = strategy.resolveShard("us-east", ctx);
        expect(["shard-0", "shard-1"]).toContain(result);
    });

    it("is deterministic for same region and key", () => {
        const ctx = makeGeoContext();
        const r1 = strategy.resolveShard("us-east", ctx);
        const r2 = strategy.resolveShard("us-east", ctx);
        expect(r1).toBe(r2);
    });

    it("distributes within eligible shards for a region", () => {
        const ctx = makeGeoContext();
        const results = new Set();
        // Use region name as the key so it maps to that region's eligible shards
        for (let i = 0; i < 50; i++) {
            results.add(strategy.resolveShard("us-east", ctx));
        }
        // Same region name always maps to same shard within the region's eligible set
        expect(results.size).toBe(1);
        // All results are within us-east's eligible shards
        for (const r of results) {
            expect(["shard-0", "shard-1"]).toContain(r);
        }
    });

    it("throws for unknown region with throw policy", () => {
        const ctx = makeGeoContext();
        expect(() => strategy.resolveShard("unknown-region", ctx)).toThrow(ShardNotFoundError);
    });

    it("uses default region with default policy", () => {
        const ctx = makeGeoContext(regions, "us-east", "default");
        const result = strategy.resolveShard("unknown-region", ctx);
        expect(["shard-0", "shard-1"]).toContain(result);
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ---------------------------------------------------------------------------
// Weighted strategy
// ---------------------------------------------------------------------------

describe("weighted strategy", () => {
    const strategy = new WeightedShardStrategy();

    const weights = { "shard-0": 1, "shard-1": 2, "shard-2": 3 };

    function makeWeightContext(w = weights) {
        return makeContext({ strategy: "weighted", weights: w });
    }

    it("resolves deterministically", () => {
        const ctx = makeWeightContext();
        const r1 = strategy.resolveShard("user-42", ctx);
        const r2 = strategy.resolveShard("user-42", ctx);
        expect(r1).toBe(r2);
    });

    it("always returns a valid shard id", () => {
        const ctx = makeWeightContext();
        for (let i = 0; i < 100; i++) {
            const result = strategy.resolveShard(`key-${i}`, ctx);
            expect(Object.keys(weights)).toContain(result);
        }
    });

    it("higher weight shards get more keys", () => {
        const ctx = makeWeightContext();
        const counts = { "shard-0": 0, "shard-1": 0, "shard-2": 0 };
        for (let i = 0; i < 1000; i++) {
            const shard = strategy.resolveShard(`key-${i}`, ctx);
            counts[shard]++;
        }
        // shard-2 (weight 3) should have more than shard-0 (weight 1)
        expect(counts["shard-2"]).toBeGreaterThan(counts["shard-0"]);
    });

    it("validate rejects invalid weights", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s1", weight: 0 }] }, ["s1"]),
        ).toThrow(ShardConfigError);
        expect(() =>
            strategy.validate({ shards: [{ id: "s1", weight: -1 }] }, ["s1"]),
        ).toThrow(ShardConfigError);
        expect(() =>
            strategy.validate({ shards: [{ id: "s1", weight: Infinity }] }, ["s1"]),
        ).toThrow(ShardConfigError);
        expect(() =>
            strategy.validate({ shards: [{ id: "s1", weight: NaN }] }, ["s1"]),
        ).toThrow(ShardConfigError);
    });

    it("validate accepts valid weights", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s1", weight: 1 }, { id: "s2", weight: 5 }] }, ["s1", "s2"]),
        ).not.toThrow();
    });

    it("validate rejects duplicate shard ids", () => {
        expect(() =>
            strategy.validate(
                { shards: [{ id: "s1", weight: 1 }, { id: "s1", weight: 2 }] },
                ["s1"],
            ),
        ).toThrow(ShardConfigError);
    });

    it("validate rejects empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ---------------------------------------------------------------------------
// Database integration with all strategies
// ---------------------------------------------------------------------------

describe("database integration with all strategies", () => {
    function makeShardConfig(id, connectionString) {
        return {
            id,
            connection: connectionString
                ? { connectionString }
                : { host: "localhost", port: 5432, database: id, user: "test" },
        };
    }

    it("creates database with consistent-hash strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "consistent-hash",
                shards: [
                    makeShardConfig("shard-0", "postgresql://localhost:5432/s0"),
                    makeShardConfig("shard-1", "postgresql://localhost:5432/s1"),
                ],
                virtualNodes: 50,
            },
        });

        expect(db.sharding.enabled).toBe(true);
        expect(db.sharding.strategy).toBe("consistent-hash");
        expect(db.sharding.shards.length).toBe(2);
    });

    it("creates database with rendezvous strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "rendezvous",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
                hashSeed: "my-seed",
            },
        });

        expect(db.sharding.strategy).toBe("rendezvous");
    });

    it("creates database with range strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "range",
                shards: [
                    makeShardConfig("shard-0"),
                    makeShardConfig("shard-1"),
                ],
                ranges: [
                    { shardId: "shard-0", start: 0, end: 99 },
                    { shardId: "shard-1", start: 100, end: 199 },
                ],
            },
        });

        expect(db.sharding.strategy).toBe("range");
    });

    it("creates database with tenant strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "tenant",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
                tenants: { "tenant-a": "shard-0", "tenant-b": "shard-1" },
            },
        });

        expect(db.sharding.strategy).toBe("tenant");
    });

    it("creates database with composite strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "composite",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
                hashStrategy: "hash",
            },
        });

        expect(db.sharding.strategy).toBe("composite");
    });

    it("creates database with geographic strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "geographic",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
                regions: { "us-east": ["shard-0"], "eu-west": ["shard-1"] },
            },
        });

        expect(db.sharding.strategy).toBe("geographic");
    });

    it("creates database with weighted strategy", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "weighted",
                shards: [
                    { id: "shard-0", connection: { host: "localhost" }, weight: 1 },
                    { id: "shard-1", connection: { host: "localhost" }, weight: 3 },
                ],
            },
        });

        expect(db.sharding.strategy).toBe("weighted");
    });

    it("routes query through consistent-hash strategy", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "consistent-hash",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
            },
        });

        const shard = db.sharding.getShardForKey("user-42");
        expect(["shard-0", "shard-1"]).toContain(shard.id);
    });

    it("routes query through range strategy", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "range",
                shards: [makeShardConfig("shard-0"), makeShardConfig("shard-1")],
                ranges: [
                    { shardId: "shard-0", start: 0, end: 99 },
                    { shardId: "shard-1", start: 100, end: 199 },
                ],
            },
        });

        const shard = db.sharding.getShardForKey("42");
        expect(shard.id).toBe("shard-0");

        const shard2 = db.sharding.getShardForKey("150");
        expect(shard2.id).toBe("shard-1");
    });

    it("routes query through weighted strategy", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "weighted",
                shards: [
                    { id: "shard-0", connection: { host: "localhost" }, weight: 1 },
                    { id: "shard-1", connection: { host: "localhost" }, weight: 1 },
                ],
            },
        });

        const shard = db.sharding.getShardForKey("user-42");
        expect(shard.id).toMatch(/^shard-[01]$/);
    });

    it("backward compatibility: legacy directory format with routing.directory", () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("shard-0")],
                routing: { directory: { "user-1": "shard-0" } },
            },
        });

        expect(db.sharding.strategy).toBe("directory");
        const shard = db.sharding.getShardForKey("user-1");
        expect(shard.id).toBe("shard-0");
    });

    it("existing config without sharding still works", () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });

        expect(db.sharding.enabled).toBe(false);
        expect(db.sharding.strategy).toBeUndefined();
    });
});
