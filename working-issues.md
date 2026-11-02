1. Core Client Layer (client/)
Package-level solvable
- [x] Configurable connection acquisition retry with exponential backoff.
- [x] Query execution timeout.
- [x] Query cancellation using PostgreSQL cancellation support.  # feasible
- [x] Configurable connection leak detection.
- [x] Better pool exhaustion diagnostics.
- [x] Connection lifecycle management.
- [x] Automatic release of clients through safe transaction helpers.
- [x] Enhanced health checks with configurable validation queries.  # feasible
- [x] Pool statistics and connection utilization APIs.
- [x] Graceful pool shutdown and draining.
- [x] Connection validation before reuse.
2. Query Layer (query/)
- [x] Query execution plan inspection using EXPLAIN.  # feasible
- [x] Optional EXPLAIN ANALYZE support.  # feasible
- [x] Query timeout configuration at builder level.
- [x] Query cancellation API.  # feasible
- [x] Query execution metadata and timing.
- [x] Automatic empty-array handling in sql.join().  # feasible (simple)
- [x] Safer SQL fragment composition.
- [x] Identifier validation helpers.
- [x] Keyset pagination.  # feasible
- [x] Cursor-based pagination.
- [x] Query result streaming integration.
- [x] Query parameter and SQL debugging utilities.
- [x] Query execution statistics.
3. Model Layer (model/)
- [x] Runtime validation integration.
- [x] Automatic model-to-schema introspection.  # feasible
- [x] Nested relationship loading.
- [x] Relationship batching to reduce N+1 queries.
- [x] Lazy relationship loading.  # feasible
- [x] Model-level pagination.
- [x] Optimistic locking using version columns.  # feasible
- [x] Automatic updated timestamp handling improvements.
- [x] Bulk upsert support.  # feasible
- [x] Model serialization and hydration improvements.  # feasible
- [x] Configurable default scopes.  # feasible
- [x] Global scopes for soft deletes.
- [x] Better transaction propagation.
- [x] Model-level query caching hooks.  # feasible
4. Schema & Migration Layer (schema/, migrate/)
- [x] Migration squashing.  # feasible
- [x] Baseline migration support.  # feasible
- [x] Migration repair commands.  # feasible
- [x] Partial migration recovery utilities.
- [x] Schema comparison and diff generation.  # feasible
- [x] Schema drift detection improvements.
- [x] Automatic dependency discovery.  # feasible
- [x] Migration execution progress reporting.  # feasible
- [x] Migration execution duration metrics.
- [x] Migration lock timeout configuration.
- [x] Migration conflict detection.
- [x] Safer destructive migration detection.
- [x] Migration history export.
- [x] Schema introspection utilities.  # feasible
5. Batch Layer (batch/)
- [x] Transaction support for updateMany().  # feasible
- [x] Transaction support for deleteMany().  # feasible
- [x] Per-row error reporting.  # feasible
- [x] Savepoint-based error isolation.  # feasible
- [x] Keyset pagination for selectMany().  # feasible
- [x] Batch progress reporting for all operations.  # feasible
- [x] Configurable retry for failed chunks.  # feasible
- [x] Adaptive batch sizing.  # feasible
- [x] Batch operation cancellation.  # feasible
- [x] Batch execution statistics.  # feasible
- [x] Batch timeout support.  # feasible
6. Sharding Layer (sharding/)
These are package-level improvements that do not require implementing a distributed management service.
- [x] Better shard key validation.
- [x] Pluggable shard selection strategies.
- [x] Weighted shard selection.
- [x] Shard health-check APIs.
- [x] Shard connection pool statistics.
- [x] Shard routing diagnostics.
- [x] Improved consistent-hash virtual-node management.
- [x] Shard topology serialization and restoration.
- [x] Rebalance plan validation.
- [x] Rebalance progress tracking primitives.
- [x] Shard-specific query execution metrics.
- [x] Improved handling of unknown shard keys.
- [x] Shard-level connection draining.
Note: Actual data movement and cross-shard coordination still require orchestration.
7. Online Resharding Layer (resharding/)
- [ ] Resharding progress percentage.  # feasible
- [ ] Estimated remaining work based on observed throughput.  # feasible
- [ ] Copy throughput metrics.  # feasible
- [x] Checkpoint inspection APIs.
- [ ] Pause and resume support.  # feasible (complex)
- [x] Configurable batch sizes.
- [x] Better interruption recovery.
- [ ] Destination schema compatibility validation.  # feasible
- [x] Row-count verification.
- [ ] Data checksum verification.  # feasible
- [ ] Cutover readiness validation.  # feasible
- [ ] Detailed resharding reports.  # feasible
- [ ] Reverse migration planning primitives.  # feasible
8. Distributed Transaction Layer (distributed-tx/)
- [ ] Configurable prepare timeout.  # feasible
- [ ] Configurable commit timeout.  # feasible
- [ ] Configurable abort timeout.  # feasible
- [ ] Retry policies for failed transaction operations.  # feasible
- [x] Transaction status inspection APIs.
- [x] Prepared transaction monitoring.
- [x] In-doubt transaction diagnostics.
- [x] Recovery worker observability.
- [x] Recovery progress reporting.
- [ ] Transaction lifecycle event hooks.  # feasible
- [ ] Participant health checks.  # feasible
- [x] Configurable recovery intervals.
- [ ] Transaction cleanup diagnostics.  # feasible
- [x] Better handling of coordinator shutdown.
Note: Retry and recovery primitives can live in the package; durable coordination and application-level business compensation remain separate concerns.
9. Replication Layer (replication/)
- [ ] Weighted replica selection.  # feasible
- [ ] Round-robin replica selection.  # feasible
- [ ] Least-connections selection.  # feasible
- [x] Replica selection based on measured lag.
- [ ] Exact WAL LSN tracking.  # feasible
- [x] Better replica lag diagnostics.
- [x] Configurable read consistency policies.
- [x] Primary fallback for unavailable replicas.
- [x] Replica health-check improvements.
- [ ] Replica selection statistics.  # feasible
- [ ] Read routing hooks.  # feasible
- [x] Configurable replica exclusion policies.
10. Failover Layer (failover/)
These improvements can be made within the package without claiming to provide a complete HA infrastructure.
- [ ] Persistent failover state storage interface.  # feasible
- [ ] Failover event history.  # feasible
- [ ] Configurable failure detection thresholds.  # feasible
- [ ] Health-check failure classification.  # feasible
- [ ] Failover state recovery after restart.  # feasible
- [ ] Failover hooks and callbacks.  # feasible
- [ ] Connection invalidation after failover.  # feasible
- [ ] Connection pool reconnection.  # feasible
- [ ] Fencing integration hooks.  # feasible
11. Streaming & Cursor Layer
- [x] Native backpressure support.  # feasible
- [x] Pause and resume streaming.  # feasible
- [x] Stream cancellation.  # feasible
- [x] Configurable cursor batch size.  # feasible
- [x] Stream error recovery.  # feasible
- [x] Stream timeout management.  # feasible
- [x] Stream progress reporting.  # feasible
- [x] Optional peer dependency detection.  # feasible
- [x] Better connection lifecycle management.