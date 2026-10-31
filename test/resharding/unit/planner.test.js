import { describe, it, expect, beforeEach } from "vitest";

import {
    topologyFromConfig,
    planResharding,
} from "../../../src/resharding/resharding-planner.js";

import {
    UnsupportedStrategyError,
    StalePlanError,
} from "../../../src/resharding/resharding-errors.js";
function shard(id, version = 1, extra = {}) {
    return {
        id,
        version,
        connection: { host: "h", port: 5432, database: id, user: "u", ...extra },
        addedAt: new Date().toISOString(),
    };
}

function topo(strategy, shards, config = {}, routingVersion = 1) {
    return {
        shards: shards.map((s) => ({ ...s, connection: s.connection })),
        strategy,
        strategyConfig: config,
        routingVersion,
        capturedAt: new Date().toISOString(),
    };
}

describe("resharding planner — comprehensive", () => {
    let source, target;

    beforeEach(() => {
        source = topo("hash", [shard("s-0"), shard("s-1")], {}, 1);
        target = topo("hash", [shard("s-0"), shard("s-1"), shard("s-2")], {}, 2);
    });

    it("produces a plan for adding a shard (hash)", async () => {
        const plan = await planResharding(source, target, "m-1");
        expect(plan.migrationId).toBe("m-1");
        expect(plan.sourceRoutingVersion).toBe(1);
        expect(plan.targetRoutingVersion).toBe(2);
        expect(plan.units.length).toBeGreaterThan(0);
        expect(plan.affectedShardIds).toContain("s-2");
        expect(plan.dryRun).toBe(false);
    });

    it("dry-run plan is marked dryRun", async () => {
        const plan = await planResharding(source, target, "m-dry", { dryRun: true });
        expect(plan.dryRun).toBe(true);
    });

    it("rejects stale plan when live routing version differs", async () => {
        await expect(
            planResharding(source, target, "m-stale", { expectedRoutingVersion: 99 }),
        ).rejects.toThrow(StalePlanError);
    });

    it("rejects unsupported strategy with typed error", async () => {
        const bad = topo("hash", [shard("s-0")], {}, 1);
        const unknown = { ...bad, strategy: "no-such-strategy" };
        await expect(planResharding(unknown, target, "m-bad")).rejects.toThrow(
            UnsupportedStrategyError,
        );
    });

    it("rejects unsupported target strategy", async () => {
        const badTarget = topo("hash", [shard("s-0")], {}, 1);
        const unknownTarget = { ...badTarget, strategy: "no-such-strategy" };
        await expect(planResharding(source, unknownTarget, "m-bad-tgt")).rejects.toThrow(
            UnsupportedStrategyError,
        );
    });

    it("directory strategy computes per-key deltas", async () => {
        const src = topo("directory", [shard("s-0"), shard("s-1")], {
            directory: { a: "s-0", b: "s-0", c: "s-1" },
        });
        const tgt = topo("directory", [shard("s-0"), shard("s-1")], {
            directory: { a: "s-1", b: "s-0", c: "s-1", d: "s-0" },
        });
        const plan = await planResharding(src, tgt, "m-dir");
        const moves = plan.units.filter((u) => u.unitType === "shard-key");
        const aMove = moves.find((u) => u.shardKey === "a");
        expect(aMove).toBeDefined();
        expect(aMove.sourceShardId).toBe("s-0");
        expect(aMove.destinationShardId).toBe("s-1");
        const dMove = moves.find((u) => u.shardKey === "d");
        expect(dMove).toBeDefined();
        expect(dMove.sourceShardId).toBe("unknown");
    });

    it("directory strategy with no changes produces no move units", async () => {
        const src = topo("directory", [shard("s-0"), shard("s-1")], {
            directory: { a: "s-0", b: "s-1" },
        });
        const tgt = topo("directory", [shard("s-0"), shard("s-1")], {
            directory: { a: "s-0", b: "s-1" },
        });
        const plan = await planResharding(src, tgt, "m-dir-nc");
        expect(plan.units.length).toBe(0);
    });

    it("tenant strategy never splits tenants; relocates whole tenant units", async () => {
        const src = topo("tenant", [shard("s-0"), shard("s-1")], {
            tenants: { t1: "s-0", t2: "s-1" },
        });
        const tgt = topo("tenant", [shard("s-0"), shard("s-1")], {
            tenants: { t1: "s-1", t2: "s-1" },
        });
        const plan = await planResharding(src, tgt, "m-ten");
        const t1 = plan.units.find((u) => u.shardKey === "t1");
        expect(t1.unitType).toBe("tenant");
        expect(t1.sourceShardId).toBe("s-0");
        expect(t1.destinationShardId).toBe("s-1");
        expect(plan.units.find((u) => u.shardKey === "t2")).toBeUndefined();
    });

    it("tenant strategy with unassigned tenant", async () => {
        const src = topo("tenant", [shard("s-0")], { tenants: { t1: "s-0" } });
        const tgt = topo("tenant", [shard("s-0"), shard("s-1")], { tenants: { t1: null } });
        const plan = await planResharding(src, tgt, "m-ten-un");
        const t1 = plan.units.find((u) => u.shardKey === "t1");
        expect(t1).toBeDefined();
        expect(t1.sourceShardId).toBe("s-0");
    });

    it("range strategy preserves boundaries and computes range deltas", async () => {
        const src = topo("range", [shard("s-0"), shard("s-1")], {
            ranges: [
                { shardId: "s-0", start: 1, end: 100 },
                { shardId: "s-1", start: 101, end: 200 },
            ],
        });
        const tgt = topo("range", [shard("s-0"), shard("s-1"), shard("s-2")], {
            ranges: [
                { shardId: "s-0", start: 1, end: 50 },
                { shardId: "s-1", start: 51, end: 150 },
                { shardId: "s-2", start: 151, end: 200 },
            ],
        });
        const plan = await planResharding(src, tgt, "m-rng");
        const rangeUnits = plan.units.filter((u) => u.unitType === "range");
        expect(rangeUnits.length).toBeGreaterThan(0);
        const s1s2 = rangeUnits.find(
            (u) => u.sourceShardId === "s-1" && u.destinationShardId === "s-2",
        );
        expect(s1s2).toBeDefined();
        expect(s1s2.rangeStart).toBe(151);
        expect(s1s2.rangeEnd).toBe(200);
    });

    it("range strategy removes a shard", async () => {
        const src = topo("range", [shard("s-0"), shard("s-1")], {
            ranges: [
                { shardId: "s-0", start: 1, end: 100 },
                { shardId: "s-1", start: 101, end: 200 },
            ],
        });
        const tgt = topo("range", [shard("s-0")], {
            ranges: [{ shardId: "s-0", start: 1, end: 200 }],
        });
        const plan = await planResharding(src, tgt, "m-rng-rm");
        const toS0 = plan.units.find((u) => u.destinationShardId === "s-0" && u.sourceShardId === "s-1");
        expect(toS0).toBeDefined();
    });

    it("consistent-hash produces bucket units for remapped nodes", async () => {
        const src = topo("consistent-hash", [shard("s-0"), shard("s-1")], {
            virtualNodes: 4,
        });
        const tgt = topo("consistent-hash", [shard("s-0"), shard("s-1"), shard("s-2")], {
            virtualNodes: 4,
        });
        const plan = await planResharding(src, tgt, "m-ch");
        const bucketUnits = plan.units.filter((u) => u.unitType === "bucket");
        expect(bucketUnits.length).toBeGreaterThan(0);
        expect(bucketUnits[0].hashBucket).toBeDefined();
    });

    it("weighted produces shard units for added/removed shards", async () => {
        const src = topo("weighted", [shard("s-0"), shard("s-1")], {
            weights: { "s-0": 1, "s-1": 1 },
        });
        const tgt = topo("weighted", [shard("s-0"), shard("s-1"), shard("s-2")], {
            weights: { "s-0": 1, "s-1": 1, "s-2": 1 },
        });
        const plan = await planResharding(src, tgt, "m-wt");
        const added = plan.units.find((u) => u.destinationShardId === "s-2");
        expect(added).toBeDefined();
    });

    it("weighted weight change produces units between common shards", async () => {
        const src = topo("weighted", [shard("s-0"), shard("s-1")], {
            weights: { "s-0": 10, "s-1": 1 },
        });
        const tgt = topo("weighted", [shard("s-0"), shard("s-1")], {
            weights: { "s-0": 1, "s-1": 10 },
        });
        const plan = await planResharding(src, tgt, "m-wt-chg");
        expect(plan.units.length).toBeGreaterThan(0);
    });

    it("rendezvous produces units for added/removed shards", async () => {
        const src = topo("rendezvous", [shard("s-0"), shard("s-1")], { hashSeed: "seed" });
        const tgt = topo("rendezvous", [shard("s-0"), shard("s-1"), shard("s-2")], { hashSeed: "seed" });
        const plan = await planResharding(src, tgt, "m-rv");
        expect(plan.units.length).toBeGreaterThan(0);
    });

    it("composite produces units for added/removed shards", async () => {
        const src = topo("composite", [shard("s-0"), shard("s-1")], {});
        const tgt = topo("composite", [shard("s-0"), shard("s-1"), shard("s-2")], {});
        const plan = await planResharding(src, tgt, "m-comp");
        expect(plan.units.length).toBeGreaterThan(0);
    });

    it("geographic produces per-region units", async () => {
        const src = topo("geographic", [shard("s-0"), shard("s-1")], {
            regions: { us: ["s-0"], eu: ["s-1"] },
        });
        const tgt = topo("geographic", [shard("s-0"), shard("s-1"), shard("s-2")], {
            regions: { us: ["s-2"], eu: ["s-1"] },
        });
        const plan = await planResharding(src, tgt, "m-geo");
        expect(plan.units.length).toBeGreaterThan(0);
    });

    it("removing a shard produces relocation units", async () => {
        const src = topo("hash", [shard("s-0"), shard("s-1")], {}, 1);
        const tgt = topo("hash", [shard("s-0")], {}, 2);
        const plan = await planResharding(src, tgt, "m-rm");
        const removed = plan.units.find((u) => u.sourceShardId === "s-1");
        expect(removed).toBeDefined();
        expect(removed.destinationShardId).toBe("s-0");
    });

    it("topologyFromConfig sanitizes credentials", () => {
        const t = topologyFromConfig({
            strategy: "hash",
            shards: [
                {
                    id: "s-0",
                    version: 1,
                    connection: {
                        host: "h",
                        port: 5432,
                        database: "db",
                        user: "u",
                        password: "secret",
                    },
                },
            ],
            routingVersion: 3,
        });
        expect(t.shards[0].connection.password).toBeUndefined();
        expect(t.shards[0].connection.host).toBe("h");
        expect(t.routingVersion).toBe(3);
    });

    it("empty source topology with added shards produces population units", async () => {
        const src = topo("hash", [], {}, 1);
        const tgt = topo("hash", [shard("s-0")], {}, 2);
        const plan = await planResharding(src, tgt, "m-empty");
        expect(plan.units.length).toBeGreaterThan(0);
    });

    it("single shard to single shard no-op produces no units", async () => {
        const src = topo("hash", [shard("s-0")], {}, 1);
        const tgt = topo("hash", [shard("s-0")], {}, 2);
        const plan = await planResharding(src, tgt, "m-noop");
        expect(plan.units.length).toBe(0);
    });
});
