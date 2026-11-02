import type {
    FencingProvider,
    FencingOptions,
    FencingActivationResult,
    FencingStatus,
} from "./fencing.js";
import type { RequiredFailoverLogger } from "./failover-types.js";

export class FencingManager {
    private readonly providers = new Map<string, FencingProvider>();

    constructor(
        private readonly defaultProvider: FencingProvider,
        private readonly logger: RequiredFailoverLogger,
    ) {
        this.providers.set("default", defaultProvider);
    }

    /**
     * Register an additional fencing provider by name.
     */
    registerProvider(name: string, provider: FencingProvider): void {
        this.providers.set(name, provider);
    }

    /**
     * Get a provider by name, falling back to the default.
     */
    use(providerName?: string): FencingProvider {
        return providerName ? (this.providers.get(providerName) ?? this.defaultProvider) : this.defaultProvider;
    }

    /**
     * Activate fencing on a node across all registered providers.
     */
    async activateAll(
        nodeId: string,
        reason: string,
        options?: FencingOptions,
    ): Promise<FencingActivationResult[]> {
        const results: FencingActivationResult[] = [];
        await this.defaultProvider.activate(nodeId, reason, options);
        results.push({ nodeId, success: true });

        for (const [name, provider] of this.providers) {
            if (name === "default") continue;
            try {
                await provider.activate(nodeId, reason, options);
                results.push({ nodeId, success: true });
            } catch (error) {
                this.logger.error(
                    { provider: name, nodeId, error: (error as Error).message },
                    "Failed to activate fencing",
                );
                results.push({
                    nodeId,
                    success: false,
                    error: (error as Error).message,
                });
            }
        }

        return results;
    }

    /**
     * Deactivate fencing on a node across all registered providers.
     */
    async deactivateAll(nodeId: string): Promise<void> {
        await this.defaultProvider.deactivate(nodeId);
        for (const [name, provider] of this.providers) {
            if (name === "default") continue;
            try {
                await provider.deactivate(nodeId);
            } catch (error) {
                this.logger.error(
                    { provider: name, nodeId, error: (error as Error).message },
                    "Failed to deactivate fencing",
                );
            }
        }
    }

    /**
     * Check if a node is fenced. Throws if the default provider says it is.
     */
    async check(nodeId: string): Promise<void> {
        await this.defaultProvider.check(nodeId);
    }

    /**
     * Returns whether the node is fenced according to the default provider.
     */
    async isFenced(nodeId: string): Promise<boolean> {
        return this.defaultProvider.isFenced(nodeId);
    }

    /**
     * Get fenced nodes from the default provider.
     */
    async getFencedNodes(): Promise<string[]> {
        return this.defaultProvider.getFencedNodes();
    }

    /**
     * Get fencing status for a node from the default provider.
     */
    async getStatus(nodeId: string): Promise<FencingStatus> {
        return this.defaultProvider.getStatus(nodeId);
    }

    /**
     * Clean up fencing for a node across all providers.
     */
    async cleanup(nodeId: string): Promise<void> {
        await this.defaultProvider.cleanup(nodeId);
        for (const [name, provider] of this.providers) {
            if (name === "default") continue;
            try {
                await provider.cleanup(nodeId);
            } catch (error) {
                this.logger.error(
                    { provider: name, nodeId, error: (error as Error).message },
                    "Failed to cleanup fencing",
                );
            }
        }
    }
}
