# @oneunit/database - Architecture Improvement Plan

## Executive Summary

This document outlines a comprehensive improvement plan for the `@oneunit/database` package, focusing on:
- **Architecture**: Cleaner, more maintainable code structure
- **Usability**: Better developer experience and API design  
- **Documentation**: Complete, professional, developer-friendly docs
- **Reliability**: Production-grade robustness and error handling
- **Performance**: Optimized for real-world use cases

---

## Current State Analysis

### Strengths
✅ Comprehensive feature set (sharding, replication, failover, distributed TX)
✅ TypeScript-first with strong typing
✅ Modular architecture (composition over inheritance)
✅ Parameterized SQL everywhere (mostly)
✅ Good separation of concerns
✅ Existing test coverage
✅ Observability hooks and metrics

### Weaknesses
⚠️ **Complexity**: 120+ files, deep nesting, many moving parts
⚠️ **Documentation**: Incomplete API reference, needs better organization
⚠️ **Configuration**: Inconsistent patterns, hardcoded values
⚠️ **Error Handling**: Some gaps in error classification and handling
⚠️ **Code Quality**: Some duplication, can improve SOLID compliance
⚠️ **Developer Experience**: Steep learning curve, needs more examples
⚠️ **Testing**: Integration tests require Docker

### Opportunities
🎯 Standardize configuration patterns
🎯 Improve type safety and inference
🎯 Better error handling and classification
🎯 Enhanced observability and metrics
🎯 Production-ready defaults
🎯 Better documentation and examples
🎯 Performance optimizations

---

## Phase 1: Architectural Analysis & Documentation

### 1.1 Complete Architecture Documentation

**Objective**: Create comprehensive architecture documentation that explains every component.

**Deliverables**:
- [ ] Complete module map with dependencies
- [ ] Data flow diagrams (Mermaid)
- [ ] Sequence diagrams for key operations
- [ ] Component interaction diagrams
- [ ] WHAT/WHY/WHERE/HOW/WHEN for every major component

**Files to Create/Update**:
- `docs/ARCHITECTURE.md` (comprehensive)
- `docs/COMPONENTS.md` (detailed component reference)
- `docs/DATA_FLOW.md` (execution flow diagrams)

---

### 1.2 API Surface Analysis

**Objective**: Catalog and document the entire public API.

**Deliverables**:
- [ ] Complete API reference with all exported functions, classes, types
- [ ] Usage examples for every public API member
- [ ] Deprecation notices for legacy APIs
- [ ] Grouping by feature area

**Files**:
- `docs/API.md`
- Update `src/index.ts` with JSDoc comments

---

## Phase 2: Core Architecture Improvements

### 2.1 Configuration System Overhaul

**Current Issues**:
- Inconsistent configuration patterns across modules
- Hardcoded timeouts and intervals
- Some options only available at construction time
- No validation of configuration values

**Proposed Improvements**:

```typescript
// New standardized configuration interface
interface DatabaseConfiguration {
  // Core
  connection: ConnectionConfig;
  pool?: PoolConfig;
  
  // Features (opt-in)
  sharding?: ShardingConfig;
  replication?: ReplicationConfig;
  failover?: FailoverConfig;
  distributedTx?: DistributedTxConfig;
  resharding?: ReshardingConfig;
  
  // Behavior
  timeouts?: TimeoutConfig;
  retry?: RetryConfig;
  validateConnections?: boolean;
  
  // Observability
  logger?: Logger;
  metrics?: MetricsConfig;
  hooks?: HookConfig;
}

interface TimeoutConfig {
  query?: number;           // Default: 30000
  connection?: number;      // Default: 5000
  connectionLeak?: number;  // Default: 300000 (5 min)
  // ... other timeouts
}
```

**Tasks**:
- [ ] Create unified configuration schema
- [ ] Add runtime validation
- [ ] Make all timeouts configurable
- [ ] Document all configuration options

---

### 2.2 Error Handling Standardization

**Current Issues**:
- Inconsistent error classification
- Some errors not properly typed
- Error messages could be more descriptive
- Missing error codes for programmatic handling

**Proposed Improvements**:

```typescript
// Standard error hierarchy
class DatabaseError extends Error {
  readonly code: string;
  readonly category: ErrorCategory;
  readonly severity: ErrorSeverity;
  readonly timestamp: Date;
  readonly context?: Record<string, unknown>;
  readonly cause?: Error;
}

// Error categories
type ErrorCategory = 
  | 'CONNECTION'
  | 'QUERY'
  | 'TRANSACTION'
  | 'CONFIGURATION'
  | 'SHARDING'
  | 'REPLICATION'
  | 'FAILOVER'
  | 'DISTRIBUTED_TX';

// Error severity
type ErrorSeverity = 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
```

**Tasks**:
- [ ] Create error hierarchy
- [ ] Standardize error codes
- [ ] Add error context (connection info, query, etc.)
- [ ] Document all error types

---

### 2.3 Connection Management Improvements

**Current Issues**:
- Global state tracking prevents GC
- Hardcoded leak detection timeout
- No connection lifecycle hooks
- Pool configuration could be more flexible

**Proposed Improvements**:

```typescript
// Use WeakRef for tracking to allow GC
const activeConnections = new WeakSet<PoolClient>();

// Configurable leak detection
interface LeakDetectionConfig {
  enabled: boolean;
  timeoutMs: number;
  warningThreshold?: number;
  autoRelease: boolean;  // Already implemented!
}

// Connection lifecycle hooks
interface ConnectionHooks {
  onAcquire?: (client: PoolClient) => void;
  onRelease?: (client: PoolClient, error?: Error) => void;
  onError?: (client: PoolClient, error: Error) => void;
  onLeak?: (client: PoolClient, duration: number) => void;
}
```

**Tasks**:
- [ ] Use WeakSet/WeakMap for connection tracking
- [ ] Make leak detection configurable
- [ ] Add connection lifecycle hooks
- [ ] Improve pool metrics

---

### 2.4 Sharding Layer Refactoring

**Current Issues**:
- Complex shard strategy implementations
- Default shard fallback can be error-prone
- Shard manager mutex could be improved
- No global connection limit enforcement

**Proposed Improvements**:

```typescript
// Global connection manager
class GlobalConnectionBudget {
  private totalLimit: number;
  private shardLimits: Map<string, number>;
  
  allocate(shardId: string, requested: number): number {
    // Fair distribution algorithm
  }
  
  release(shardId: string, count: number): void {
    // Release connections
  }
}

// Improved shard manager with async-safe mutex
class ShardManager {
  private mutex = new AsyncMutex();
  
  async addShard(config: ShardConfig): Promise<void> {
    const release = await this.mutex.acquire();
    try {
      // Critical section
    } finally {
      release();
    }
  }
}
```

**Tasks**:
- [ ] Implement global connection budget
- [ ] Replace mutex with AsyncMutex
- [ ] Add shard health checks
- [ ] Improve shard selection algorithms

---

## Phase 3: Developer Experience Improvements

### 3.1 Public API Simplification

**Current Issues**:
- Large public API surface (150+ exports)
- Some exports should be internal
- Inconsistent naming conventions
- Missing utility functions

**Proposed Improvements**:

```typescript
// Before: 150+ exports
export * from "./database.js";
export * from "./client/client.js";
// ... 50+ more

// After: Curated exports
// Core
export { createDatabase } from "./database.js";
export type { Database, DatabaseConfig } from "./types.js";

// Client
export { createClient, createPool } from "./client/index.js";
export type { ClientApi, PoolConfig } from "./client/types.js";

// Query Builder
export { sql, raw, QueryBuilder } from "./query/index.js";

// Sharding (opt-in)
export { createShardManager } from "./sharding/index.js";
export type { ShardConfig, ShardingStrategy } from "./sharding/types.js";

// Sub-path exports for advanced use
// "@oneunit/database/client"
// "@oneunit/database/sharding"
// etc.
```

**Tasks**:
- [ ] Curate public API exports
- [ ] Create sub-path exports
- [ ] Add deprecation warnings for legacy exports
- [ ] Document migration path

---

### 3.2 Type Safety Enhancements

**Current Issues**:
- Some types could be more specific
- Generic types not fully utilized
- Missing type guards
- Some any/unknown types

**Proposed Improvements**:

```typescript
// Strongly typed query results
export interface QueryResult<T = Record<string, unknown>> {
  rows: T[];
  rowCount: number;
  // ...
}

// Type-safe query builder
class QueryBuilder<T = Record<string, unknown>> {
  private table: string;
  private whereConditions: Array<{ column: string; operator: string; value: unknown }> = [];
  
  select<Columns extends keyof T>(columns: Columns[]): QueryBuilder<Pick<T, Columns>> {
    // Type-safe select
  }
  
  where<Key extends keyof T>(column: Key, value: T[Key]): this {
    // Type-safe where
  }
}

// Type guards
function isDatabaseError(error: unknown): error is DatabaseError {
  return error instanceof DatabaseError;
}

function isTransientError(error: unknown): error is DatabaseError & { transient: true } {
  return isDatabaseError(error) && error.transient;
}
```

**Tasks**:
- [ ] Improve generic type usage
- [ ] Add type guards for error types
- [ ] Strengthen QueryBuilder types
- [ ] Improve type inference

---

### 3.3 Enhanced Observability

**Current Issues**:
- Metrics collection is opt-in
- No built-in Prometheus support
- Health checks could be more comprehensive
- Missing distributed tracing integration

**Proposed Improvements**:

```typescript
// Standardized metrics interface
interface MetricsCollector {
  increment(name: string, labels?: Record<string, string>, value?: number): void;
  decrement(name: string, labels?: Record<string, string>, value?: number): void;
  histogram(name: string, labels?: Record<string, string>, value: number): void;
  gauge(name: string, labels?: Record<string, string>, value: number): void;
}

// Prometheus adapter
class PrometheusCollector implements MetricsCollector {
  private client: promClient.CollectDefaultMetrics;
  
  increment(name: string, labels: Record<string, string> = {}, value: number = 1): void {
    this.client.counter(name, labels).inc(value);
  }
  // ...
}

// Health check enhancements
interface HealthCheckResult {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: HealthCheck[];
  timestamp: Date;
  duration: number;
}

interface HealthCheck {
  name: string;
  status: 'up' | 'down';
  latency?: number;
  error?: string;
  details?: Record<string, unknown>;
}
```

**Tasks**:
- [ ] Standardize metrics interface
- [ ] Add Prometheus adapter
- [ ] Enhance health checks
- [ ] Add distributed tracing hooks

---

## Phase 4: Documentation Overhaul

### 4.1 README.md Rewrite

**Structure**:

```markdown
# @oneunit/database

## 📖 Table of Contents
- Introduction
- Features
- Installation
- Quick Start
- Configuration
- Core Concepts
- API Reference
- Examples
- Architecture
- Production Guide
- Troubleshooting
- Contributing
- License

## 🚀 Introduction
- What is @oneunit/database?
- Why use it?
- Key features
- Use cases

## ✨ Features
- Connection pooling
- Query builder
- Transactions
- Sharding
- Replication
- Failover
- Distributed transactions
- Online resharding
- Observability

## 📦 Installation
- NPM
- Yarn
- PNPM
- Requirements

## 🏃 Quick Start
- Basic usage
- Common patterns
- First application

## ⚙️ Configuration
- Connection configuration
- Pool configuration
- Feature flags
- Timeouts
- SSL/TLS
- Environment variables

## 🧠 Core Concepts
- Database instance
- Connection management
- Query execution
- Transaction handling
- Sharding strategies
- Replication modes
- Failover states

## 📚 API Reference
- createDatabase()
- Database
- Client
- QueryBuilder
- Model
- Schema
- Sharding
- Replication
- Distributed Transactions
- Failover
- Resharding

## 💡 Examples
- Basic CRUD
- Transactions
- Sharding
- Replication
- Failover
- Distributed transactions
- Resharding

## 🏗️ Architecture
- Overview
- Module map
- Data flow
- Design principles
- Dependency graph

## 🏭 Production Guide
- Deployment
- Configuration
- Monitoring
- Scaling
- Security
- Troubleshooting

## ❓ FAQ
- Common questions
- Best practices
- Known limitations
```

**Tasks**:
- [ ] Rewrite README.md with professional structure
- [ ] Add comprehensive examples
- [ ] Include architecture diagrams
- [ ] Document all features

---

### 4.2 Module Documentation

Create dedicated documentation for each major module:

```
 docs/
 ├── README.md                  # Main package documentation
 ├── ARCHITECTURE.md            # Architecture overview
 ├── API/
 │   ├── client.md              # Client API
 │   ├── query-builder.md       # Query Builder API
 │   ├── sharding.md            # Sharding API
 │   ├── replication.md         # Replication API
 │   ├── failover.md            # Failover API
 │   ├── distributed-tx.md      # Distributed TX API
 │   └── resharding.md          # Resharding API
 ├── GUIDES/
 │   ├── getting-started.md     # Getting Started
 │   ├── configuration.md       # Configuration Guide
 │   ├── production.md          # Production Guide
 │   ├── performance.md         # Performance Guide
 │   └── troubleshooting.md     # Troubleshooting
 └── EXAMPLES/
     ├── basic.md               # Basic Examples
     ├── sharding.md            # Sharding Examples
     └── distributed-tx.md      # Distributed TX Examples
```

**Tasks**:
- [ ] Create module documentation structure
- [ ] Document each module with WHAT/WHY/WHERE/HOW/WHEN
- [ ] Add examples for each module
- [ ] Include diagrams where helpful

---

## Phase 5: Testing & Quality Improvements

### 5.1 Test Coverage

**Current State**:
- Unit tests for many modules
- Integration tests require Docker
- Some edge cases may be missing

**Proposed Improvements**:
- [ ] Add mock PostgreSQL for faster unit tests
- [ ] Improve test organization
- [ ] Add property-based tests for query builder
- [ ] Add stress tests for connection pooling
- [ ] Add failover scenario tests
- [ ] Document testing strategy

---

### 5.2 Code Quality

**Tasks**:
- [ ] Run ESLint/Prettier consistently
- [ ] Add more JSDoc comments
- [ ] Improve code organization
- [ ] Remove dead code
- [ ] Fix TODOs and FIXMEs
- [ ] Add code review checklist

---

## Phase 6: Performance Optimizations

### 6.1 Connection Pooling
- [ ] Optimize pool size based on workload
- [ ] Implement connection reuse strategies
- [ ] Add pool metrics (hit rate, wait time, etc.)

### 6.2 Query Execution
- [ ] Implement query caching for repeated queries
- [ ] Optimize prepared statement handling
- [ ] Add query timeout optimizations

### 6.3 Sharding
- [ ] Optimize shard routing
- [ ] Implement connection caching per shard
- [ ] Add shard health-based routing

---

## Implementation Priority

### 🔴 High Priority (Must Have)
1. Configuration system overhaul (2.1)
2. Error handling standardization (2.2)
3. README.md rewrite (4.1)
4. API surface simplification (3.1)
5. Connection management improvements (2.3)

### 🟡 Medium Priority (Should Have)
1. Module documentation (4.2)
2. Type safety enhancements (3.2)
3. Enhanced observability (3.3)
4. Sharding layer refactoring (2.4)
5. Test coverage improvements (5.1)

### 🟢 Low Priority (Nice to Have)
1. Performance optimizations (6)
2. Code quality improvements (5.2)
3. Advanced features and utilities

---

## Timeline Estimate

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| Phase 1: Analysis & Documentation | 2-3 days | Architecture docs, API analysis |
| Phase 2: Core Architecture | 3-4 days | Configuration, errors, connections |
| Phase 3: Developer Experience | 2-3 days | API simplification, types, observability |
| Phase 4: Documentation | 3-4 days | README, module docs, examples |
| Phase 5: Testing & Quality | 2-3 days | Tests, code quality, reviews |
| Phase 6: Performance | 2-3 days | Optimizations, benchmarks |
| **Total** | **14-20 days** | Complete package improvement |

---

## Success Criteria

✅ All public APIs documented with examples  
✅ All error types classified and documented  
✅ All configuration options documented  
✅ Architecture clearly explained with diagrams  
✅ Type safety improved  
✅ Code quality improved (ESLint, Prettier, JSDoc)  
✅ Test coverage maintained or improved  
✅ Performance optimized for common use cases  
✅ Developer experience significantly improved  
✅ Production readiness verified  

---

## Next Steps

1. **Review and approve this plan**
2. **Prioritize phases based on urgency**
3. **Assign resources to each phase**
4. **Begin implementation with Phase 1**
5. **Iterate and refine based on feedback**

---

## Appendix: Key Files to Review

### Core
- `src/database.ts` - Main database factory
- `src/types.ts` - All type definitions
- `src/index.ts` - Public API exports

### Client
- `src/client/*.ts` - Connection, pool, query execution

### Sharding
- `src/sharding/shard-manager.ts`
- `src/sharding/shard-router.ts`
- `src/sharding/strategies.ts`

### Distributed TX
- `src/distributed-tx/distributed-transaction-coordinator.ts`
- `src/distributed-tx/distributed-transaction-manager.ts`

### Replication & Failover
- `src/replication/replication-manager.ts`
- `src/failover/failover-manager.ts`

---

*Document Version: 1.0*  
*Last Updated: 2026-11-03*  
*Status: Draft - Awaiting Review*
