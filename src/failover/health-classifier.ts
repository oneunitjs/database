/**
 * Category of a health-check failure.
 */
export type HealthCheckFailureCategory =
    | "connection_refused"
    | "connection_timeout"
    | "connection_reset"
    | "authentication_failed"
    | "query_timeout"
    | "query_error"
    | "network_unreachable"
    | "dns_resolution_failed"
    | "server_error"
    | "unknown";

/**
 * Severity of a classified failure.
 */
export type HealthCheckFailureSeverity = "transient" | "persistent" | "critical";

/**
 * Result of classifying a health-check failure.
 */
export interface ClassifiedFailure {
    category: HealthCheckFailureCategory;
    severity: HealthCheckFailureSeverity;
    retryable: boolean;
    failoverImmediately: boolean;
    description: string;
}

/**
 * A failure observed during a health check.
 */
export interface HealthCheckFailure {
    error: Error;
    nodeId: string;
    shardId: string;
    timestamp: Date;
    durationMs: number;
}

/**
 * Base classification metadata shared across instances of the same category.
 */
interface CategoryMetadata {
    category: HealthCheckFailureCategory;
    severity: HealthCheckFailureSeverity;
    retryable: boolean;
    failoverImmediately: boolean;
}

/**
 * Default category metadata used when a failure cannot be matched more precisely.
 */
const DEFAULT_CATEGORY: CategoryMetadata = {
    category: "unknown",
    severity: "transient",
    retryable: true,
    failoverImmediately: false,
};

/**
 * Default mapping from error category to classification metadata.
 */
const DEFAULT_CATEGORY_MAP: Map<HealthCheckFailureCategory, CategoryMetadata> = new Map([
    ["connection_refused", { category: "connection_refused", severity: "critical", retryable: true, failoverImmediately: true }],
    ["connection_timeout", { category: "connection_timeout", severity: "transient", retryable: true, failoverImmediately: false }],
    ["connection_reset", { category: "connection_reset", severity: "persistent", retryable: true, failoverImmediately: false }],
    ["authentication_failed", { category: "authentication_failed", severity: "critical", retryable: false, failoverImmediately: false }],
    ["query_timeout", { category: "query_timeout", severity: "transient", retryable: true, failoverImmediately: false }],
    ["query_error", { category: "query_error", severity: "persistent", retryable: false, failoverImmediately: false }],
    ["network_unreachable", { category: "network_unreachable", severity: "persistent", retryable: true, failoverImmediately: false }],
    ["dns_resolution_failed", { category: "dns_resolution_failed", severity: "persistent", retryable: false, failoverImmediately: false }],
    ["server_error", { category: "server_error", severity: "critical", retryable: false, failoverImmediately: true }],
    ["unknown", { category: "unknown", severity: "transient", retryable: true, failoverImmediately: false }],
]);

/**
 * Classifies health-check failures into categories with severity, retryability,
 * and failover-immediately flags. Errors are matched by error code and message
 * content.
 */
export class HealthCheckClassifier {
    private readonly categoryMap: Map<HealthCheckFailureCategory, CategoryMetadata>;

    constructor(categoryMap?: Map<HealthCheckFailureCategory, Omit<CategoryMetadata, 'category'>>) {
        this.categoryMap = new Map(DEFAULT_CATEGORY_MAP);
        if (categoryMap) {
            for (const [category, meta] of categoryMap) {
                this.categoryMap.set(category, { ...meta, category });
            }
        }
    }

    /**
     * Classify a health-check failure.
     */
    classify(failure: HealthCheckFailure): ClassifiedFailure {
        const category = this.categorizeError(failure.error);
        const base = this.categoryMap.get(category) ?? DEFAULT_CATEGORY;
        return {
            ...base,
            category,
            description: this.describeFailure(failure, category),
        };
    }

    /**
     * Returns whether a classified failure should trigger failover immediately.
     */
    shouldFailoverImmediately(classification: ClassifiedFailure): boolean {
        return classification.failoverImmediately;
    }

    /**
     * Returns whether a classified failure is retryable.
     */
    isRetryable(classification: ClassifiedFailure): boolean {
        return classification.retryable;
    }

    /**
     * Returns the severity of a classified failure.
     */
    getSeverity(classification: ClassifiedFailure): HealthCheckFailureSeverity {
        return classification.severity;
    }

    /**
     * Override or extend the classification metadata for a category.
     */
    setCategoryMetadata(
        category: HealthCheckFailureCategory,
        meta: Omit<CategoryMetadata, 'category'>,
    ): void {
        this.categoryMap.set(category, { ...meta, category });
    }

    private categorizeError(error: Error): HealthCheckFailureCategory {
        const message = error.message.toLowerCase();
        const code = (error as NodeJS.ErrnoException).code;

        if (code === "ECONNREFUSED") return "connection_refused";
        if (code === "ETIMEDOUT") return "connection_timeout";
        if (code === "ECONNRESET") return "connection_reset";
        if (code === "ENOTFOUND" || code === "EAI_AGAIN") return "dns_resolution_failed";
        if (code === "ENETUNREACH" || code === "EHOSTUNREACH") return "network_unreachable";
        if (code === "28P01" || code === "28000") return "authentication_failed";

        if (
            message.includes("authentication failed") ||
            message.includes("28p01") ||
            message.includes("28000")
        ) {
            return "authentication_failed";
        }
        if (message.includes("connection refused")) return "connection_refused";
        if (message.includes("connection reset")) return "connection_reset";
        if (message.includes("connection timed out")) return "connection_timeout";
        if (message.includes("query timeout") || message.includes("statement timeout")) return "query_timeout";
        if (message.includes("does not exist") || code?.startsWith?.("42")) return "query_error";
        if (message.includes("connection closed") || message.includes("timeout")) return "connection_timeout";
        if (message.includes("server error") || message.includes("server closed")) return "server_error";
        return "unknown";
    }

    private describeFailure(failure: HealthCheckFailure, category: HealthCheckFailureCategory): string {
        const code = (failure.error as NodeJS.ErrnoException).code;
        const detail = code ? ` (code: ${code})` : "";
        return `Health check failed on node "${failure.nodeId}" [${category}]: ${failure.error.message}${detail}`;
    }
}

/**
 * Default health-check classifier instance.
 */
export const defaultHealthCheckClassifier = new HealthCheckClassifier();
