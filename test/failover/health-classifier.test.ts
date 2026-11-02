import { describe, it, expect, beforeEach } from "vitest";
import {
    HealthCheckClassifier,
    defaultHealthCheckClassifier,
} from "../../src/failover/health-classifier.js";

function makeFailure(message: string, code?: string) {
    const error = new Error(message);
    (error as any).code = code;
    return {
        error,
        nodeId: "node-1",
        shardId: "shard-1",
        timestamp: new Date(),
        durationMs: 100,
    };
}

describe("HealthCheckClassifier", () => {
    let classifier: HealthCheckClassifier;

    beforeEach(() => {
        classifier = new HealthCheckClassifier();
    });

    it("classifies ECONNREFUSED as connection_refused / critical / failoverImmediately", () => {
        const result = classifier.classify(makeFailure("connect ECONNREFUSED", "ECONNREFUSED"));
        expect(result.category).toBe("connection_refused");
        expect(result.severity).toBe("critical");
        expect(result.retryable).toBe(true);
        expect(result.failoverImmediately).toBe(true);
    });

    it("classifies ETIMEDOUT as connection_timeout / transient", () => {
        const result = classifier.classify(makeFailure("connect ETIMEDOUT", "ETIMEDOUT"));
        expect(result.category).toBe("connection_timeout");
        expect(result.severity).toBe("transient");
        expect(result.retryable).toBe(true);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies ECONNRESET as connection_reset / persistent", () => {
        const result = classifier.classify(makeFailure("connect ECONNRESET", "ECONNRESET"));
        expect(result.category).toBe("connection_reset");
        expect(result.severity).toBe("persistent");
        expect(result.retryable).toBe(true);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies authentication failed message as authentication_failed / critical", () => {
        const result = classifier.classify(makeFailure("password authentication failed for user", "28P01"));
        expect(result.category).toBe("authentication_failed");
        expect(result.severity).toBe("critical");
        expect(result.retryable).toBe(false);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies query timeout as query_timeout / transient", () => {
        const result = classifier.classify(makeFailure("canceling statement due to statement timeout"));
        expect(result.category).toBe("query_timeout");
        expect(result.severity).toBe("transient");
        expect(result.retryable).toBe(true);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies query error as query_error / persistent", () => {
        const result = classifier.classify(makeFailure("relation \"nonexistent\" does not exist", "42P01"));
        expect(result.category).toBe("query_error");
        expect(result.severity).toBe("persistent");
        expect(result.retryable).toBe(false);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies ENETUNREACH as network_unreachable / persistent", () => {
        const result = classifier.classify(makeFailure("connect ENETUNREACH", "ENETUNREACH"));
        expect(result.category).toBe("network_unreachable");
        expect(result.severity).toBe("persistent");
        expect(result.retryable).toBe(true);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies ENOTFOUND as dns_resolution_failed / persistent", () => {
        const result = classifier.classify(makeFailure("getaddrinfo ENOTFOUND", "ENOTFOUND"));
        expect(result.category).toBe("dns_resolution_failed");
        expect(result.severity).toBe("persistent");
        expect(result.retryable).toBe(false);
        expect(result.failoverImmediately).toBe(false);
    });

    it("classifies server error message as server_error / critical / failoverImmediately", () => {
        const result = classifier.classify(makeFailure("server closed the connection unexpectedly"));
        expect(result.category).toBe("server_error");
        expect(result.severity).toBe("critical");
        expect(result.retryable).toBe(false);
        expect(result.failoverImmediately).toBe(true);
    });

    it("classifies unknown error as unknown / transient", () => {
        const result = classifier.classify(makeFailure("some weird error"));
        expect(result.category).toBe("unknown");
        expect(result.severity).toBe("transient");
        expect(result.retryable).toBe(true);
        expect(result.failoverImmediately).toBe(false);
    });

    it("shouldFailoverImmediately returns the flag", () => {
        const critical = classifier.classify(makeFailure("connect ECONNREFUSED", "ECONNREFUSED"));
        expect(classifier.shouldFailoverImmediately(critical)).toBe(true);

        const transient = classifier.classify(makeFailure("connect ETIMEDOUT", "ETIMEDOUT"));
        expect(classifier.shouldFailoverImmediately(transient)).toBe(false);
    });

    it("isRetryable returns the flag", () => {
        const auth = classifier.classify(makeFailure("auth failed", "28P01"));
        expect(classifier.isRetryable(auth)).toBe(false);

        const timeout = classifier.classify(makeFailure("timeout", "ETIMEDOUT"));
        expect(classifier.isRetryable(timeout)).toBe(true);
    });

    it("getSeverity returns the severity", () => {
        const result = classifier.classify(makeFailure("connect ECONNREFUSED", "ECONNREFUSED"));
        expect(classifier.getSeverity(result)).toBe("critical");
    });

    it("description includes nodeId and category", () => {
        const result = classifier.classify(makeFailure("connect ECONNREFUSED", "ECONNREFUSED"));
        expect(result.description).toContain("node-1");
        expect(result.description).toContain("connection_refused");
        expect(result.description).toContain("connect ECONNREFUSED");
    });

    it("defaultHealthCheckClassifier is a working instance", () => {
        const result = defaultHealthCheckClassifier.classify(makeFailure("connect ECONNREFUSED", "ECONNREFUSED"));
        expect(result.category).toBe("connection_refused");
    });

    it("setCategoryMetadata overrides a category", () => {
        classifier.setCategoryMetadata("connection_timeout", {
            severity: "critical",
            retryable: false,
            failoverImmediately: true,
        });
        const result = classifier.classify(makeFailure("timeout", "ETIMEDOUT"));
        expect(result.severity).toBe("critical");
        expect(result.retryable).toBe(false);
        expect(result.failoverImmediately).toBe(true);
    });
});
