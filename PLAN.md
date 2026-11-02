# @oneunit/database - Production Readiness Implementation Plan

> **Status:** Draft | **Target:** Production-Ready Release | **Estimated:** 14-20 weeks

This document outlines the implementation plan to address all missing features required for production deployment.

---

## 🎯 Priority Levels

| Priority | Level | Description | Target Phase |
|----------|-------|-------------|--------------|
| P0 | **Critical** | Blocks production use, data safety risk | Phase 1 |
| P1 | **High** | Required for production, performance/correctness | Phase 2 |
| P2 | **Medium** | Nice-to-have, operational improvements | Phase 3 |

---

## 📋 Phase 1: Critical Features (P0) - Weeks 1-8

### 🔴 1.1 Failover Layer Foundation (Weeks 1-4)
**Why:** Without failover, the package cannot achieve high availability.

#### 1.1.1 Persistent Failover State Storage
- **Effort:** 3-5 days | **File:** `src/failover/failover-state-store.ts`
- **Approach:** Create interface + PostgreSQL implementation
- **Schema:** `_database_failover_state` and `_database_failover_events` tables
- **Acceptance:** State persists across restarts, atomic updates

#### 1.1.2 State Recovery After Restart
- **Effort:** 3-5 days | **File:** `src/failover/failover-manager.ts`
- **Approach:** Load state on initialization, version-based concurrency
- **Acceptance:** Multiple instances coordinate via shared store

#### 1.1.3 Connection Invalidation
- **Effort:** 3-5 days | **File:** `src/failover/connection-tracker.ts`
- **Approach:** Track connections per shard, invalidate on failover
- **Acceptance:** No connection leaks, graceful cleanup

#### 1.1.4 Health-Check Failure Classification
- **Effort:** 2-3 days | **File:** `src/failover/health-classifier.ts`
- **Approach:** Classify errors (connection-refused, timeout, auth, etc.)
- **Acceptance:** All error types properly classified

---

### 🔴 1.2 Distributed Transaction Timeout Configuration (Weeks 5-6)
**Why:** Without timeouts, transactions can hang indefinitely.

#### 1.2.1 Configurable Timeouts
- **Effort:** 3-4 days | **File:** `src/distributed-tx/distributed-transaction-coordinator.ts`
- **Approach:** Add `prepareTimeoutMs`, `commitTimeoutMs`, `abortTimeoutMs`, `globalTimeoutMs`
- **Acceptance:** All phases have configurable timeouts, proper cleanup

#### 1.2.2 Retry Policies
- **Effort:** 3-4 days | **File:** `src/distributed-tx/retry-policy.ts`
- **Approach:** Exponential backoff, configurable max retries, retryable error classification
- **Acceptance:** Retry on transient errors only, no infinite loops

---

### 🔴 1.3 Integration & Testing (Weeks 7-8)
- **Integration testing** of Failover + Distributed TX
- **Performance testing** under failure conditions
- **Bug fixes** from testing
- **Documentation** updates

**Acceptance Criteria for Phase 1:**
- [ ] Failover layer functional with persistent state
- [ ] Distributed transactions have proper timeout/retry handling
- [ ] Package maintains HA during primary failures
- [ ] 100% test coverage for new features

---

## 📋 Phase 2: High Priority Features (P1) - Weeks 9-14

### 🟡 2.1 Replication Layer Enhancements (Weeks 9-10)

#### 2.1.1 Weighted/Round-Robin/Least-Connections Selection
- **Effort:** 8-12 days total
- **Files:** `src/replication/replication-router.ts`
- **Approach:**
  - **Weighted:** Probability proportional to weight
  - **Round-Robin:** Sequential cycling
  - **Least-Connections:** Select replica with fewest active connections
- **Acceptance:** Even distribution, configurable per-replica

#### 2.1.2 Exact WAL LSN Tracking
- **Effort:** 5-7 days
- **File:** `src/replication/replication-manager.ts`
- **Approach:** Parse PostgreSQL LSN, calculate byte difference
- **Acceptance:** Accurate lag in bytes, works across PG versions

#### 2.1.3 Replica Selection Statistics
- **Effort:** 2-3 days
- **File:** `src/replication/replication-stats.ts`
- **Approach:** Track requests, latency, success/failure per replica
- **Acceptance:** Accurate metrics, rolling averages

#### 2.1.4 Read Routing Hooks
- **Effort:** 3-4 days
- **File:** `src/replication/replication-router.ts`
- **Approach:** `beforeSelect` and `afterSelect` hooks with full context
- **Acceptance:** Hooks can modify routing, multiple hooks supported

---

### 🟡 2.2 Failover Layer Completion (Weeks 11-12)

#### 2.2.1 Failover Event History
- **Effort:** 3-4 days
- **File:** `src/failover/failover-state-store.ts`
- **Approach:** Persist all state changes with timestamps
- **Acceptance:** Queryable events, automatic cleanup

#### 2.2.2 Configurable Failure Detection Thresholds
- **Effort:** 3-4 days
- **File:** `src/failover/failover-config.ts`
- **Approach:** Per-node thresholds for consecutive failures, unhealthy duration
- **Acceptance:** Configurable per node, proper state transitions

#### 2.2.3 Failover Hooks & Callbacks
- **Effort:** 3-4 days
- **File:** `src/failover/failover-hooks.ts`
- **Approach:** `onStateChange`, `onPrimaryChange`, `onTopologyChange`, `onHealthCheck`
- **Acceptance:** All events trigger hooks, errors don't break failover

#### 2.2.4 Fencing Integration
- **Effort:** 5-7 days
- **File:** `src/failover/fencing.ts`
- **Approach:**
  - Interface: `FencingProvider`
  - Implementations: `AdvisoryLockFencingProvider`, `TableBasedFencingProvider`
  - Integration: Block writes during failover
- **Acceptance:** Prevents writes during failover, token-based validation

---

### 🟡 2.3 Integration & Testing (Weeks 13-14)
- **Integration testing** of all Phase 2 features
- **Load testing** for replication/failover scenarios
- **Bug fixes** from testing
- **Documentation** updates

**Acceptance Criteria for Phase 2:**
- [ ] Replication has smart load balancing
- [ ] Failover has comprehensive event tracking
- [ ] Failover has fencing integration
- [ ] Package handles read scaling efficiently
- [ ] 100% test coverage for new features

---

## 📋 Phase 3: Medium Priority Features (P2) - Weeks 15-20

### 🟠 3.1 Resharding Enhancements (Weeks 15-17)

#### 3.1.1 Resharding Progress Percentage
- **Effort:** 3-4 days | **File:** `src/sharding/shard-rebalance-tracker.ts`
- **Approach:** Calculate percentage from keys/rows/bytes copied
- **Acceptance:** Real-time percentage, works for all metrics

#### 3.1.2 Copy Throughput Metrics
- **Effort:** 3-4 days | **File:** `src/sharding/shard-rebalance-tracker.ts`
- **Approach:** Sliding window calculation for keys/sec, bytes/sec, rows/sec
- **Acceptance:** Accurate throughput, configurable window

#### 3.1.3 Pause and Resume Support
- **Effort:** 5-7 days | **Files:** `src/sharding/shard-rebalance-tracker.ts`, `src/resharding/resharding-manager.ts`
- **Approach:** Add `paused` state, pause/resume methods, reason tracking
- **Acceptance:** Works with actual operations, proper state transitions

#### 3.1.4 Schema Compatibility Validation
- **Effort:** 5-7 days | **File:** `src/resharding/schema-validator.ts`
- **Approach:** Compare tables, columns, constraints, indexes between shards
- **Acceptance:** Detects all incompatibilities, provides suggestions

#### 3.1.5 Data Checksum Verification
- **Effort:** 5-7 days | **File:** `src/resharding/checksum-verifier.ts`
- **Approach:** Batched checksum calculation, comparison between shards
- **Acceptance:** Accurate verification, works for large tables

#### 3.1.6 Cutover Readiness Validation
- **Effort:** 5-7 days | **File:** `src/resharding/cutover-validator.ts`
- **Approach:** Check data copy, schema, integrity, lag, health before cutover
- **Acceptance:** Blocks on blockers, allows warnings, provides detailed report

#### 3.1.7 Detailed Resharding Reports
- **Effort:** 3-4 days | **File:** `src/resharding/resharding-reporter.ts`
- **Approach:** Generate markdown/JSON reports with statistics, progress, issues
- **Acceptance:** Multiple output formats, report listing/filtering

#### 3.1.8 Reverse Migration Planning
- **Effort:** 3-4 days | **File:** `src/resharding/reverse-migration.ts`
- **Approach:** Generate plan with dependencies, risks, steps
- **Acceptance:** Complete plan, identifies all issues, provides mitigation

---

### 🟠 3.2 Distributed Transaction Enhancements (Weeks 18-19)

#### 3.2.1 Transaction Lifecycle Event Hooks
- **Effort:** 3-4 days | **File:** `src/distributed-tx/hooks.ts`
- **Approach:** Hooks for all lifecycle events + participant-level hooks
- **Acceptance:** All events trigger hooks, errors don't break transactions

#### 3.2.2 Participant Health Checks
- **Effort:** 3-4 days | **File:** `src/distributed-tx/participant-health.ts`
- **Approach:** Periodic health checks, track status, integrate with coordinator
- **Acceptance:** Blocks operations on unhealthy participants

#### 3.2.3 Transaction Cleanup Diagnostics
- **Effort:** 3-4 days | **File:** `src/distributed-tx/cleanup-diagnostics.ts`
- **Approach:** Scan prepared transactions, identify abandoned, provide cleanup
- **Acceptance:** Detects all prepared transactions, safe cleanup

---

### 🟠 3.3 Integration & Testing (Week 20)
- **Final integration testing** of all features
- **Performance benchmarking**
- **Documentation completion**
- **Release preparation**

**Acceptance Criteria for Phase 3:**
- [ ] Resharding layer is production-ready
- [ ] All medium-priority features implemented
- [ ] Comprehensive documentation updated
- [ ] All tests passing

---

## 📅 Implementation Roadmap

### Phase 1: Critical (Weeks 1-8)
| Week | Task | Files |
|------|------|-------|
| 1-2 | Failover: Persistent State Storage | `failover-state-store.ts` |
| 2-3 | Failover: State Recovery | `failover-manager.ts` |
| 3-4 | Failover: Connection Invalidation | `connection-tracker.ts` |
| 3-4 | Failover: Health Classification | `health-classifier.ts` |
| 5-6 | Distributed TX: Timeouts | `distributed-transaction-coordinator.ts` |
| 5-6 | Distributed TX: Retry Policies | `retry-policy.ts` |
| 7-8 | **Integration Testing** | All |

### Phase 2: High Priority (Weeks 9-14)
| Week | Task | Files |
|------|------|-------|
| 9-10 | Replication: Load Balancing | `replication-router.ts` |
| 9-10 | Replication: WAL LSN Tracking | `replication-manager.ts` |
| 9-10 | Replication: Stats + Hooks | `replication-stats.ts` |
| 11-12 | Failover: Event History | `failover-state-store.ts` |
| 11-12 | Failover: Thresholds | `failover-config.ts` |
| 11-12 | Failover: Hooks + Callbacks | `failover-hooks.ts` |
| 13-14 | Failover: Fencing | `fencing.ts` |
| 13-14 | **Integration Testing** | All |

### Phase 3: Medium Priority (Weeks 15-20)
| Week | Task | Files |
|------|------|-------|
| 15-17 | Resharding: All features | `resharding/*.ts` |
| 18-19 | Distributed TX: Hooks + Health + Cleanup | `distributed-tx/*.ts` |
| 20 | **Final Integration** | All |

---

## 🎯 Success Criteria

### Phase 1 Completion
- ✅ Failover layer functional with persistent state
- ✅ Distributed transactions have proper timeout/retry handling
- ✅ Package maintains HA during primary failures
- ✅ All critical bugs fixed
- ✅ 100% test coverage for new features

### Phase 2 Completion
- ✅ Replication has smart load balancing
- ✅ Failover has comprehensive event tracking
- ✅ Failover has fencing integration
- ✅ Package handles read scaling efficiently
- ✅ All high-priority bugs fixed
- ✅ 100% test coverage for new features

### Phase 3 Completion
- ✅ Resharding layer is production-ready
- ✅ All medium-priority features implemented
- ✅ Comprehensive documentation updated
- ✅ Performance benchmarks meet targets
- ✅ All tests passing

### Final Release
- ✅ All working-issues.md items checked [x]
- ✅ Full test suite passing (1000+ tests)
- ✅ Documentation complete
- ✅ Performance benchmarks documented
- ✅ Migration guides provided

---

## 👥 Resources

### Team Composition
- 1 Tech Lead / Architect
- 2-3 Senior Backend Engineers
- 1 QA Engineer
- 1 Technical Writer

### Required Skills
- PostgreSQL expertise (WAL, replication, prepared transactions)
- Distributed systems experience
- TypeScript/Node.js development
- Testing (unit, integration, e2e)

### Tools
- PostgreSQL 12+
- Node.js 20+
- TypeScript 5+
- Vitest
- Docker

---

## 🚀 Next Steps

1. Review and approve this plan
2. Assign team members to phases
3. Set up project tracking (Jira/GitHub)
4. Create milestones for each phase
5. Begin Phase 1 implementation

---

## 📚 References

- [working-issues.md](./working-issues.md) - Feature tracking
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Technical architecture
- [README.md](./README.md) - Usage documentation

### Detailed Implementation Plans
- [failover-plan.md](./failover-plan.md) - Failover Layer detailed implementation
- [distributed-tx-plan.md](./distributed-tx-plan.md) - Distributed Transaction Layer detailed implementation
- [replication-plan.md](./replication-plan.md) - Replication Layer detailed implementation
- [resharding-plan.md](./resharding-plan.md) - Resharding Layer detailed implementation
