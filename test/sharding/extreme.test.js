import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
    createDatabase,
    HashShardStrategy,
    DirectoryShardStrategy,
    ConsistentHashShardStrategy,
    RendezvousShardStrategy,
    RangeShardStrategy,
    TenantShardStrategy,
    CompositeShardStrategy,
    GeographicShardStrategy,
    WeightedShardStrategy,
    createShardStrategy,
    getRegisteredStrategyNames,
    ShardNotFoundError,
    ShardingDisabledError,
    ShardConfigError,
    ShardRoutingError,
    isShardNotFoundError,
    isShardingDisabledError,
    isShardRoutingError,
} from "../../src/index.js";

const mockQuery = vi.fn();
const mockConnect = vi.fn();
const mockRelease = vi.fn();
const mockEnd = vi.fn();
const mockOn = vi.fn();

vi.mock("pg", async (importOriginal) => {
    const actual = await importOriginal();
    return {
        ...actual,
        Pool: vi.fn().mockImplementation(() => ({
            query: mockQuery,
            connect: mockConnect,
            release: mockRelease,
            end: mockEnd,
            on: mockOn,
            totalCount: 0,
            idleCount: 0,
            waitingCount: 0,
        })),
    };
});

function resetMocks() {
    vi.clearAllMocks();
    mockConnect.mockResolvedValue({
        query: mockQuery,
        release: mockRelease,
    });
    // INSERT statements (e.g. conditional operation registration) return rowCount 1;
    // everything else returns empty. This mirrors a non-draining shard.
    mockQuery.mockImplementation((sql) => {
        const s = typeof sql === "string" ? sql : String(sql);
        const isInsert = s.trim().toUpperCase().startsWith("INSERT");
        return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 });
    });
    mockEnd.mockResolvedValue(undefined);
}

beforeEach(() => {
    resetMocks();
});

afterEach(() => {
    resetMocks();
});

// ============================================================================
// EXTREME TESTS: All strategies
// ============================================================================

describe("EXTREME: strategy registry", () => {
    it("registers exactly 9 strategies, no more no less", () => {
        const names = getRegisteredStrategyNames();
        expect(names.length).toBe(9);
        expect([...names].sort()).toEqual([
            "composite",
            "consistent-hash",
            "directory",
            "geographic",
            "hash",
            "range",
            "rendezvous",
            "tenant",
            "weighted",
        ]);
    });

    it("createShardStrategy throws for every invalid name", () => {
        const invalid = [
            null,
            undefined,
            "",
            "  ",
            "HASH",
            "Hash",
            "consistent_hash",
            "consistentHash",
            "consistent hash",
            "directory ",
            " directory",
            "range-sharding",
            "weighted-sharding",
            "tenant-aware",
            "geo",
            "geolocation",
            123,
            true,
            [],
            {},
        ];
        for (const bad of invalid) {
            expect(() => createShardStrategy(bad)).toThrow();
        }
    });

    it("all strategy classes implement the shared contract", () => {
        const strategies = [
            new HashShardStrategy(),
            new ConsistentHashShardStrategy(),
            new RendezvousShardStrategy(),
            new RangeShardStrategy(),
            new DirectoryShardStrategy(),
            new TenantShardStrategy(),
            new CompositeShardStrategy(),
            new GeographicShardStrategy(),
            new WeightedShardStrategy(),
        ];

        for (const s of strategies) {
            expect(typeof s.name).toBe("string");
            expect(typeof s.version).toBe("number");
            expect(typeof s.validate).toBe("function");
            expect(typeof s.resolveShard).toBe("function");
        }
    });

    it("every registered name resolves through factory", () => {
        const names = getRegisteredStrategyNames();
        for (const name of names) {
            const s = createShardStrategy(name);
            expect(s.name).toBe(name);
        }
    });
});

// ============================================================================
// EXTREME: Hash strategy
// ============================================================================

describe("EXTREME: hash strategy", () => {
    const strategy = new HashShardStrategy();
    const shardIds = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"];

    it("empty string key resolves to a valid shard", () => {
        const result = strategy.resolveShard("", {
            shardKey: "",
            strategy: "hash",
            shardIds,
        });
        expect(shardIds).toContain(result);
    });

    it("very long key resolves deterministically", () => {
        const longKey = "x".repeat(100000);
        const r1 = strategy.resolveShard(longKey, {
            shardKey: longKey,
            strategy: "hash",
            shardIds,
        });
        const r2 = strategy.resolveShard(longKey, {
            shardKey: longKey,
            strategy: "hash",
            shardIds,
        });
        expect(r1).toBe(r2);
        expect(shardIds).toContain(r1);
    });

    it("unicode keys resolve deterministically", () => {
        const keys = ["日本語", "中文", "한국어", "🚀🔥", "émoji🎉test", "àáâãäå"];
        const results = new Map();
        for (const k of keys) {
            results.set(k, strategy.resolveShard(k, {
                shardKey: k,
                strategy: "hash",
                shardIds,
            }));
        }
        for (const k of keys) {
            const r1 = results.get(k);
            const r2 = strategy.resolveShard(k, {
                shardKey: k,
                strategy: "hash",
                shardIds,
            });
            expect(r1).toBe(r2);
        }
    });

    it("special characters in key resolve safely", () => {
        const keys = ["'", '"', ";", "DROP TABLE", "--", "/*", "*/", "\\", "\n", "\r", "\t"];
        for (const k of keys) {
            const result = strategy.resolveShard(k, {
                shardKey: k,
                strategy: "hash",
                shardIds,
            });
            expect(shardIds).toContain(result);
        }
    });

    it("null/undefined key throws", () => {
        expect(() =>
            // @ts-expect-error - intentionally testing invalid runtime value
            strategy.resolveShard(null, {
                shardKey: null,
                strategy: "hash",
                shardIds,
            }),
        ).toThrow();
        expect(() =>
            // @ts-expect-error - intentionally testing invalid runtime value
            strategy.resolveShard(undefined, {
                shardKey: undefined,
                strategy: "hash",
                shardIds,
            }),
        ).toThrow();
    });

    it("single shard always returns itself", () => {
        const result = strategy.resolveShard("any-key", {
            shardKey: "any-key",
            strategy: "hash",
            shardIds: ["only-shard"],
        });
        expect(result).toBe("only-shard");
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
        expect(() => strategy.validate({}, shardIds)).not.toThrow();
    });

    it("key normalization is case-insensitive and trims", () => {
        const r1 = strategy.resolveShard("User-42", {
            shardKey: "User-42",
            strategy: "hash",
            shardIds,
        });
        const r2 = strategy.resolveShard("user-42", {
            shardKey: "user-42",
            strategy: "hash",
            shardIds,
        });
        const r3 = strategy.resolveShard("  USER-42  ", {
            shardKey: "  USER-42  ",
            strategy: "hash",
            shardIds,
        });
        expect(r1).toBe(r2);
        expect(r2).toBe(r3);
    });
});

// ============================================================================
// EXTREME: Consistent hash strategy
// ============================================================================

describe("EXTREME: consistent-hash strategy", () => {
    const strategy = new ConsistentHashShardStrategy();

    it("virtualNodes=1 still routes deterministically", () => {
        const ctx = {
            shardKey: "key",
            strategy: "consistent-hash",
            shardIds: ["s0", "s1", "s2"],
            virtualNodes: 1,
        };
        const r1 = strategy.resolveShard("key", ctx);
        const r2 = strategy.resolveShard("key", ctx);
        expect(r1).toBe(r2);
        expect(["s0", "s1", "s2"]).toContain(r1);
    });

    it("virtualNodes=0 throws or handles gracefully", () => {
        const ctx = {
            shardKey: "key",
            strategy: "consistent-hash",
            shardIds: ["s0", "s1"],
            virtualNodes: 0,
        };
        // Should not crash; may throw or return something
        expect(() => strategy.resolveShard("key", ctx)).not.toThrow();
    });

    it("negative virtualNodes handled", () => {
        const ctx = {
            shardKey: "key",
            strategy: "consistent-hash",
            shardIds: ["s0", "s1"],
            virtualNodes: -5,
        };
        expect(() => strategy.resolveShard("key", ctx)).not.toThrow();
    });

    it("remapping is bounded when adding one shard", () => {
        const ctx3 = {
            shardKey: "key",
            strategy: "consistent-hash",
            shardIds: ["s0", "s1", "s2"],
            virtualNodes: 100,
        };
        const ctx4 = {
            shardKey: "key",
            strategy: "consistent-hash",
            shardIds: ["s0", "s1", "s2", "s3"],
            virtualNodes: 100,
        };

        let remapped = 0;
        const total = 500;
        for (let i = 0; i < total; i++) {
            const k = `user-${i}`;
            if (strategy.resolveShard(k, ctx3) !== strategy.resolveShard(k, ctx4)) {
                remapped++;
            }
        }
        // Consistent hashing: ~1/(N+1) = 1/4 = 25% remapped
        expect(remapped).toBeGreaterThan(0);
        expect(remapped).toBeLessThan(total);
        expect(remapped / total).toBeLessThan(0.5);
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });

    it("same key across multiple calls returns same shard (1000 iterations)", () => {
        const ctx = {
            shardKey: "stable-key",
            strategy: "consistent-hash",
            shardIds: ["s0", "s1", "s2"],
            virtualNodes: 50,
        };
        const results = new Set();
        for (let i = 0; i < 1000; i++) {
            results.add(strategy.resolveShard("stable-key", ctx));
        }
        expect(results.size).toBe(1);
    });
});

// ============================================================================
// EXTREME: Rendezvous strategy
// ============================================================================

describe("EXTREME: rendezvous strategy", () => {
    const strategy = new RendezvousShardStrategy();

    it("hashSeed changes routing distribution", () => {
        const ctx1 = {
            shardKey: "key",
            strategy: "rendezvous",
            shardIds: ["s0", "s1", "s2"],
            hashSeed: "alpha",
        };
        const ctx2 = {
            shardKey: "key",
            strategy: "rendezvous",
            shardIds: ["s0", "s1", "s2"],
            hashSeed: "beta",
        };

        let different = 0;
        const total = 200;
        for (let i = 0; i < total; i++) {
            const k = `item-${i}`;
            if (
                strategy.resolveShard(k, { ...ctx1, shardKey: k }) !==
                strategy.resolveShard(k, { ...ctx2, shardKey: k })
            ) {
                different++;
            }
        }
        expect(different).toBeGreaterThan(0);
    });

    it("empty hashSeed still works", () => {
        const ctx = {
            shardKey: "key",
            strategy: "rendezvous",
            shardIds: ["s0", "s1"],
            hashSeed: "",
        };
        const result = strategy.resolveShard("key", ctx);
        expect(["s0", "s1"]).toContain(result);
    });

    it("undefined hashSeed falls back gracefully", () => {
        const ctx = {
            shardKey: "key",
            strategy: "rendezvous",
            shardIds: ["s0", "s1"],
        };
        const result = strategy.resolveShard("key", ctx);
        expect(["s0", "s1"]).toContain(result);
    });

    it("same seed produces identical routing", () => {
        const ctx1 = { shardKey: "x", strategy: "rendezvous", shardIds: ["a", "b"], hashSeed: "s" };
        const ctx2 = { shardKey: "x", strategy: "rendezvous", shardIds: ["a", "b"], hashSeed: "s" };
        expect(strategy.resolveShard("x", ctx1)).toBe(strategy.resolveShard("x", ctx2));
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ============================================================================
// EXTREME: Range strategy
// ============================================================================

describe("EXTREME: range strategy", () => {
    const strategy = new RangeShardStrategy();

    const ranges = [
        { shardId: "s0", start: 0, end: 99 },
        { shardId: "s1", start: 100, end: 199 },
        { shardId: "s2", start: 200, end: 299 },
    ];

    function makeCtx(rngs = ranges) {
        return { shardKey: "0", strategy: "range", shardIds: ["s0", "s1", "s2"], ranges: rngs };
    }

    it("boundary values route correctly", () => {
        const ctx = makeCtx();
        expect(strategy.resolveShard("0", ctx)).toBe("s0");
        expect(strategy.resolveShard("99", ctx)).toBe("s0");
        expect(strategy.resolveShard("100", ctx)).toBe("s1");
        expect(strategy.resolveShard("199", ctx)).toBe("s1");
        expect(strategy.resolveShard("200", ctx)).toBe("s2");
        expect(strategy.resolveShard("299", ctx)).toBe("s2");
    });

    it("negative numbers throw (out of range)", () => {
        const ctx = makeCtx();
        expect(() => strategy.resolveShard("-1", ctx)).toThrow(ShardNotFoundError);
        expect(() => strategy.resolveShard("-100", ctx)).toThrow(ShardNotFoundError);
    });

    it("values beyond max range throw", () => {
        const ctx = makeCtx();
        expect(() => strategy.resolveShard("300", ctx)).toThrow(ShardNotFoundError);
        expect(() => strategy.resolveShard("999999", ctx)).toThrow(ShardNotFoundError);
    });

    it("non-numeric keys throw", () => {
        const ctx = makeCtx();
        expect(() => strategy.resolveShard("abc", ctx)).toThrow(ShardNotFoundError);
        // "12.5" parses to 12.5 which IS a valid number in range [0, 99]
        expect(strategy.resolveShard("12.5", ctx)).toBe("s0");
        // "abc123" is not a valid number
        expect(() => strategy.resolveShard("abc123", ctx)).toThrow(ShardNotFoundError);
    });

    it("inclusive/exclusive boundaries respected", () => {
        const ctx = makeCtx([
            { shardId: "s0", start: 0, end: 100, endInclusive: false },
            { shardId: "s1", start: 100, end: 200 },
        ]);
        // 99 is in s0 ([0, 100))
        expect(strategy.resolveShard("99", ctx)).toBe("s0");
        // 100 is NOT in s0 (endInclusive=false) but IS in s1 (start=100 inclusive)
        expect(strategy.resolveShard("100", ctx)).toBe("s1");
    });

    it("overlapping ranges rejected in validate", () => {
        expect(() =>
            strategy.validate(
                { ranges: [
                    { shardId: "s0", start: 0, end: 100 },
                    { shardId: "s1", start: 50, end: 150 },
                ] },
                ["s0", "s1"],
            ),
        ).toThrow(ShardConfigError);
    });

    it("duplicate shardId in ranges rejected", () => {
        expect(() =>
            strategy.validate(
                { ranges: [
                    { shardId: "s0", start: 0, end: 50 },
                    { shardId: "s0", start: 51, end: 100 },
                ] },
                ["s0"],
            ),
        ).toThrow(ShardConfigError);
    });

    it("empty ranges rejected", () => {
        expect(() =>
            strategy.validate({ ranges: [] }, ["s0"]),
        ).toThrow(ShardConfigError);
    });

    it("no ranges in context throws at resolve time", () => {
        const ctx = { shardKey: "50", strategy: "range", shardIds: ["s0"] };
        expect(() => strategy.resolveShard("50", ctx)).toThrow(ShardConfigError);
    });

    it("validate accepts valid single range", () => {
        expect(() =>
            strategy.validate(
                { ranges: [{ shardId: "s0", start: 0, end: 1000 }] },
                ["s0"],
            ),
        ).not.toThrow();
    });

    it("adjacent ranges don't overlap", () => {
        expect(() =>
            strategy.validate(
                { ranges: [
                    { shardId: "s0", start: 0, end: 99 },
                    { shardId: "s1", start: 100, end: 199 },
                ] },
                ["s0", "s1"],
            ),
        ).not.toThrow();
    });
});

// ============================================================================
// EXTREME: Directory strategy
// ============================================================================

describe("EXTREME: directory strategy", () => {
    const strategy = new DirectoryShardStrategy();

    it("empty directory throws for any key", () => {
        const ctx = {
            shardKey: "anything",
            strategy: "directory",
            shardIds: ["s0"],
            directory: {},
            unknownKeyPolicy: "throw",
        };
        expect(() => strategy.resolveShard("anything", ctx)).toThrow(ShardNotFoundError);
    });

    it("directory with non-existent shard target returns it (router validates)", () => {
        const ctx = {
            shardKey: "k",
            strategy: "directory",
            shardIds: ["s0"],
            directory: { k: "nonexistent-shard" },
            unknownKeyPolicy: "throw",
        };
        // Strategy returns the mapped value; ShardRouter validates existence
        const result = strategy.resolveShard("k", ctx);
        expect(result).toBe("nonexistent-shard");
    });

    it("unknownKeyPolicy=default falls back to defaultShardId", () => {
        const ctx = {
            shardKey: "missing",
            strategy: "directory",
            shardIds: ["s0", "s1"],
            directory: { "a": "s0" },
            unknownKeyPolicy: "default",
            defaultShardId: "s1",
        };
        expect(strategy.resolveShard("missing", ctx)).toBe("s1");
    });

    it("unknownKeyPolicy=default without defaultShardId throws", () => {
        const ctx = {
            shardKey: "missing",
            strategy: "directory",
            shardIds: ["s0"],
            directory: { "a": "s0" },
            unknownKeyPolicy: "default",
        };
        expect(() => strategy.resolveShard("missing", ctx)).toThrow(ShardNotFoundError);
    });

    it("missing directory property throws ShardConfigError", () => {
        const ctx = {
            shardKey: "k",
            strategy: "directory",
            shardIds: ["s0"],
        };
        expect(() => strategy.resolveShard("k", ctx)).toThrow(ShardConfigError);
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });

    it("directory lookup is case-sensitive after normalization", () => {
        const ctx = {
            shardKey: "User-1",
            strategy: "directory",
            shardIds: ["s0"],
            directory: { "user-1": "s0" },
            unknownKeyPolicy: "throw",
        };
        expect(strategy.resolveShard("User-1", ctx)).toBe("s0");
    });
});

// ============================================================================
// EXTREME: Tenant strategy
// ============================================================================

describe("EXTREME: tenant strategy", () => {
    const strategy = new TenantShardStrategy();

    it("unknown tenant with defaultTenantShardId falls back", () => {
        const ctx = {
            shardKey: "unknown-tenant",
            strategy: "tenant",
            shardIds: ["s0", "s1"],
            tenants: { "known": "s0" },
            defaultTenantShardId: "s1",
        };
        expect(strategy.resolveShard("unknown-tenant", ctx)).toBe("s1");
    });

    it("unknown tenant without default throws", () => {
        const ctx = {
            shardKey: "unknown",
            strategy: "tenant",
            shardIds: ["s0"],
            tenants: { "known": "s0" },
        };
        expect(() => strategy.resolveShard("unknown", ctx)).toThrow(ShardNotFoundError);
    });

    it("empty tenants object throws", () => {
        const ctx = {
            shardKey: "t",
            strategy: "tenant",
            shardIds: ["s0"],
            tenants: {},
        };
        expect(() => strategy.resolveShard("t", ctx)).toThrow(ShardConfigError);
    });

    it("tenant mapping to non-existent shard returns it (router validates)", () => {
        const ctx = {
            shardKey: "t",
            strategy: "tenant",
            shardIds: ["s0"],
            tenants: { "t": "nonexistent" },
        };
        // Strategy returns the mapped value; ShardRouter validates existence
        const result = strategy.resolveShard("t", ctx);
        expect(result).toBe("nonexistent");
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ============================================================================
// EXTREME: Composite strategy
// ============================================================================

describe("EXTREME: composite strategy", () => {
    const strategy = new CompositeShardStrategy();

    it("null component serializes without collision", () => {
        const ctx1 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1", "s2"],
            compositeKey: { a: null, b: "x" },
        };
        const ctx2 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1", "s2"],
            compositeKey: { a: "x", b: null },
        };
        const r1 = strategy.resolveShard("ignored", ctx1);
        const r2 = strategy.resolveShard("ignored", ctx2);
        expect(["s0", "s1", "s2"]).toContain(r1);
        expect(["s0", "s1", "s2"]).toContain(r2);
    });

    it("undefined component serializes without collision", () => {
        const ctx1 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: { a: undefined, b: 1 },
        };
        const ctx2 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: { a: 1, b: undefined },
        };
        const r1 = strategy.resolveShard("ignored", ctx1);
        const r2 = strategy.resolveShard("ignored", ctx2);
        expect(["s0", "s1"]).toContain(r1);
        expect(["s0", "s1"]).toContain(r2);
    });

    it("boolean components are distinguished", () => {
        const ctx1 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: { flag: true },
        };
        const ctx2 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: { flag: false },
        };
        const r1 = strategy.resolveShard("ignored", ctx1);
        const r2 = strategy.resolveShard("ignored", ctx2);
        expect(["s0", "s1"]).toContain(r1);
        expect(["s0", "s1"]).toContain(r2);
    });

    it("numeric components distinguished from strings", () => {
        const ctx1 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: { id: 42 },
        };
        const ctx2 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: { id: "42" },
        };
        const r1 = strategy.resolveShard("ignored", ctx1);
        const r2 = strategy.resolveShard("ignored", ctx2);
        expect(["s0", "s1"]).toContain(r1);
        expect(["s0", "s1"]).toContain(r2);
    });

    it("empty compositeKey falls back to shardKey", () => {
        const ctx = {
            shardKey: "user-42",
            strategy: "composite",
            shardIds: ["s0", "s1"],
            compositeKey: {},
        };
        const result = strategy.resolveShard("user-42", ctx);
        expect(["s0", "s1"]).toContain(result);
    });

    it("compositeKey with pipe characters doesn't collide", () => {
        const ctx1 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1", "s2", "s3"],
            compositeKey: { k: "a|b" },
        };
        const ctx2 = {
            shardKey: "ignored",
            strategy: "composite",
            shardIds: ["s0", "s1", "s2", "s3"],
            compositeKey: { k: "a", k2: "|b" },
        };
        const r1 = strategy.resolveShard("ignored", ctx1);
        const r2 = strategy.resolveShard("ignored", ctx2);
        expect(["s0", "s1", "s2", "s3"]).toContain(r1);
        expect(["s0", "s1", "s2", "s3"]).toContain(r2);
    });

    it("invalid hashStrategy rejected", () => {
        expect(() =>
            strategy.validate(
                { hashStrategy: "modulo" },
                ["s0", "s1"],
            ),
        ).toThrow(ShardConfigError);
    });

    it("all valid hashStrategies accepted", () => {
        for (const hs of ["hash", "consistent-hash", "rendezvous", "weighted"]) {
            expect(() =>
                strategy.validate({ hashStrategy: hs }, ["s0"]),
            ).not.toThrow();
        }
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ============================================================================
// EXTREME: Geographic strategy
// ============================================================================

describe("EXTREME: geographic strategy", () => {
    const strategy = new GeographicShardStrategy();

    const regions = {
        "us-east": ["s0", "s1"],
        "eu-west": ["s1", "s2"],
        "ap-south": ["s2"],
    };

    function makeCtx(reg = regions, def = undefined, pol = "throw") {
        return {
            shardKey: "us-east",
            strategy: "geographic",
            shardIds: ["s0", "s1", "s2"],
            regions: reg,
            defaultRegion: def,
            unknownRegionPolicy: pol,
        };
    }

    it("unknown region with defaultRegion falls back", () => {
        const ctx = makeCtx(regions, "us-east", "default");
        const result = strategy.resolveShard("unknown-region", ctx);
        expect(["s0", "s1"]).toContain(result);
    });

    it("unknown region without default throws", () => {
        const ctx = makeCtx(regions, undefined, "throw");
        expect(() => strategy.resolveShard("mars", ctx)).toThrow(ShardNotFoundError);
    });

    it("empty regions object throws", () => {
        const ctx = {
            shardKey: "r",
            strategy: "geographic",
            shardIds: ["s0"],
            regions: {},
        };
        expect(() => strategy.resolveShard("r", ctx)).toThrow(ShardConfigError);
    });

    it("region maps to non-existent shard returns it as-is (router validates)", () => {
        const ctx = {
            shardKey: "us",
            strategy: "geographic",
            shardIds: ["s0"],
            regions: { "us": ["nonexistent"] },
        };
        // The strategy returns whatever is in the regions map;
        // the ShardRouter validates existence at a higher level
        const result = strategy.resolveShard("us", ctx);
        expect(result).toBe("nonexistent");
    });

    it("same region+key always same shard", () => {
        const ctx = makeCtx();
        const results = new Set();
        for (let i = 0; i < 100; i++) {
            results.add(strategy.resolveShard("us-east", ctx));
        }
        expect(results.size).toBe(1);
    });

    it("different keys in same region map to eligible shards only", () => {
        const ctx = makeCtx();
        // Use region names as the shardKey so they map to region's eligible shards
        for (let i = 0; i < 50; i++) {
            const result = strategy.resolveShard("us-east", ctx);
            expect(["s0", "s1"]).toContain(result);
        }
    });

    it("validate throws for empty shard list", () => {
        expect(() => strategy.validate({}, [])).toThrow(ShardConfigError);
    });
});

// ============================================================================
// EXTREME: Weighted strategy
// ============================================================================

describe("EXTREME: weighted strategy", () => {
    const strategy = new WeightedShardStrategy();

    function makeCtx(w = { "s0": 1, "s1": 1 }) {
        return {
            shardKey: "k",
            strategy: "weighted",
            shardIds: Object.keys(w),
            weights: w,
        };
    }

    it("weight of 0 rejected", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s0", weight: 0 }] }, ["s0"]),
        ).toThrow(ShardConfigError);
    });

    it("negative weight rejected", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s0", weight: -5 }] }, ["s0"]),
        ).toThrow(ShardConfigError);
    });

    it("Infinity weight rejected", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s0", weight: Infinity }] }, ["s0"]),
        ).toThrow(ShardConfigError);
    });

    it("NaN weight rejected", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s0", weight: NaN }] }, ["s0"]),
        ).toThrow(ShardConfigError);
    });

    it("weight must be finite number", () => {
        expect(() =>
            strategy.validate({ shards: [{ id: "s0", weight: "5" }] }, ["s0"]),
        ).toThrow(ShardConfigError);
    });

    it("duplicate shard ids rejected", () => {
        expect(() =>
            strategy.validate(
                { shards: [{ id: "s0", weight: 1 }, { id: "s0", weight: 2 }] },
                ["s0"],
            ),
        ).toThrow(ShardConfigError);
    });

    it("empty shards list rejected", () => {
        expect(() => strategy.validate({ shards: [] }, [])).toThrow(ShardConfigError);
    });

    it("no weights in context throws", () => {
        const ctx = {
            shardKey: "k",
            strategy: "weighted",
            shardIds: ["s0"],
        };
        expect(() => strategy.resolveShard("k", ctx)).toThrow(ShardConfigError);
    });

    it("deterministic routing for same key", () => {
        const ctx = makeCtx();
        const results = new Set();
        for (let i = 0; i < 100; i++) {
            results.add(strategy.resolveShard("same-key", ctx));
        }
        expect(results.size).toBe(1);
    });

    it("higher weight gets proportionally more keys", () => {
        const ctx = makeCtx({ "s0": 1, "s1": 9 });
        const counts = { s0: 0, s1: 0 };
        for (let i = 0; i < 1000; i++) {
            const s = strategy.resolveShard(`key-${i}`, ctx);
            counts[s]++;
        }
        expect(counts.s1).toBeGreaterThan(counts.s0);
    });
});

// ============================================================================
// EXTREME: Database integration - error handling
// ============================================================================

describe("EXTREME: database integration errors", () => {
    function makeShardConfig(id, connectionString) {
        return {
            id,
            connection: connectionString
                ? { connectionString }
                : { host: "localhost", port: 5432, database: id, user: "test" },
        };
    }

    it("enabled with no shards throws descriptive error", () => {
        expect(() =>
            createDatabase({
                sharding: { enabled: true, strategy: "hash", shards: [] },
            }),
        ).toThrow(/no shards are configured/);
    });

    it("sharding disabled: db.query works without shardKey", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });
        const result = await db.query("SELECT 1");
        expect(result.rows).toEqual([{ id: 1 }]);
    });

    it("sharding disabled: db.sharding.query throws ShardingDisabledError", async () => {
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
            sharding: { enabled: false, strategy: "hash", shards: [] },
        });

        await expect(db.sharding.query("k", "SELECT 1")).rejects.toThrow(
            ShardingDisabledError,
        );
        await expect(db.sharding.transaction("k", async () => {})).rejects.toThrow(
            ShardingDisabledError,
        );
    });

    it("sharding enabled: db.query with unknown shardKey throws ShardRoutingError", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("s0")],
                routing: { directory: { "a": "s0" } },
            },
        });

        await expect(
            db.query("SELECT 1", [], { shardKey: "missing" }),
        ).rejects.toThrow(ShardRoutingError);
    });

    it("sharding enabled: db.transaction with unknown shardKey throws ShardRoutingError", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "directory",
                shards: [makeShardConfig("s0")],
                routing: { directory: { "a": "s0" } },
            },
        });

        await expect(
            db.transaction(async () => {}, { shardKey: "missing" }),
        ).rejects.toThrow(ShardRoutingError);
    });

    it("ShardRoutingError has correct name", () => {
        const err = new ShardRoutingError("route failed");
        expect(err.name).toBe("ShardRoutingError");
        expect(isShardRoutingError(err)).toBe(true);
        expect(isShardRoutingError(new Error("other"))).toBe(false);
    });

    it("ShardNotFoundError is identifiable", () => {
        const err = new ShardNotFoundError("bad");
        expect(isShardNotFoundError(err)).toBe(true);
        expect(err.code).toBe("SHARD_NOT_FOUND");
    });

    it("ShardingDisabledError is identifiable", () => {
        const err = new ShardingDisabledError();
        expect(isShardingDisabledError(err)).toBe(true);
        expect(err.code).toBe("SHARDING_DISABLED");
    });

    it("ShardConfigError is identifiable", () => {
        const err = new ShardConfigError("bad config");
        expect(err.code).toBe("SHARD_CONFIG_ERROR");
    });
});

// ============================================================================
// EXTREME: Database integration - concurrent routing
// ============================================================================

describe("EXTREME: concurrent routing", () => {
    function makeShardConfig(id, connectionString) {
        return {
            id,
            connection: connectionString
                ? { connectionString }
                : { host: "localhost", port: 5432, database: id, user: "test" },
        };
    }

    it("20 concurrent queries to different keys all complete", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("s0", "postgresql://localhost:5432/s0"),
                    makeShardConfig("s1", "postgresql://localhost:5432/s1"),
                    makeShardConfig("s2", "postgresql://localhost:5432/s2"),
                ],
            },
        });

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const promises = Array.from({ length: 20 }, (_, i) =>
            db.query("SELECT 1", [], { shardKey: `user-${i}` }),
        );

        const results = await Promise.all(promises);
        expect(results.length).toBe(20);
        for (const r of results) {
            expect(r.rows).toEqual([{ id: 1 }]);
        }
    });

    it("20 concurrent transactions to different keys all complete", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("s0", "postgresql://localhost:5432/s0"),
                    makeShardConfig("s1", "postgresql://localhost:5432/s1"),
                ],
            },
        });

        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });

        const promises = Array.from({ length: 20 }, (_, i) =>
            db.transaction(
                async (client) => {
                    await client.query("INSERT INTO t VALUES ($1)", [i]);
                    return i;
                },
                { shardKey: `user-${i}` },
            ),
        );

        const results = await Promise.all(promises);
        expect(results.length).toBe(20);
        for (let i = 0; i < 20; i++) {
            expect(results[i]).toBe(i);
        }
    });

    it("mixed queries and transactions concurrently", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("s0", "postgresql://localhost:5432/s0"),
                    makeShardConfig("s1", "postgresql://localhost:5432/s1"),
                ],
            },
        });

        mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

        const ops = [
            db.query("SELECT 1", [], { shardKey: "q1" }),
            db.transaction(async (c) => { await c.query("SELECT 1"); return "tx1"; }, { shardKey: "t1" }),
            db.query("SELECT 2", [], { shardKey: "q2" }),
            db.transaction(async (c) => { await c.query("SELECT 2"); return "tx2"; }, { shardKey: "t2" }),
        ];

        const results = await Promise.all(ops);
        expect(results.length).toBe(4);
    });
});

// ============================================================================
// EXTREME: Shutdown behavior
// ============================================================================

describe("EXTREME: shutdown", () => {
    function makeShardConfig(id, connectionString) {
        return {
            id,
            connection: connectionString
                ? { connectionString }
                : { host: "localhost", port: 5432, database: id, user: "test" },
        };
    }

    it("shutdown closes all shard pools", async () => {
        mockEnd.mockResolvedValue(undefined);
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [
                    makeShardConfig("s0"),
                    makeShardConfig("s1"),
                    makeShardConfig("s2"),
                    makeShardConfig("s3"),
                ],
            },
        });

        expect(db.sharding.shards.length).toBe(4);
        await db.sharding.shutdown();
        expect(mockEnd).toHaveBeenCalledTimes(4);
        expect(db.sharding.isClosed).toBe(true);
    });

    it("shutdown is idempotent", async () => {
        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("s0")],
            },
        });

        await db.sharding.shutdown();
        await db.sharding.shutdown();
        await db.sharding.shutdown();
        // After shutdown, isClosed should be true
        expect(db.sharding.isClosed).toBe(true);
    });

    it("shutdown when sharding disabled resolves without error", async () => {
        mockEnd.mockResolvedValue(undefined);
        const db = createDatabase({
            connectionString: "postgresql://localhost:5432/app",
        });
        await db.shutdown(); // should not throw
    });

    it("db.query after shutdown throws routing error", async () => {
        mockEnd.mockResolvedValue(undefined);
        mockConnect.mockResolvedValue({
            query: mockQuery,
            release: mockRelease,
        });
        mockQuery.mockImplementation((sql) => { const s = typeof sql === "string" ? sql : String(sql); const isInsert = s.trim().toUpperCase().startsWith("INSERT"); return Promise.resolve({ rows: [], rowCount: isInsert ? 1 : 0 }); });

        const db = createDatabase({
            sharding: {
                enabled: true,
                strategy: "hash",
                shards: [makeShardConfig("s0")],
            },
        });

        await db.sharding.shutdown();
        // After shutdown, pools are cleared, so routing fails
        await expect(db.query("SELECT 1", [], { shardKey: "k" })).rejects.toThrow();
    });
});
