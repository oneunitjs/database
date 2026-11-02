# Resharding Layer Implementation Plan

> **Status:** Draft | **Priority:** P1/P2 | **Target:** Weeks 15-17

Detailed implementation for 9 unimplemented features from working-issues.md section 7.

---

## 📋 Overview

9 features for Online Resharding Layer (resharding/):
1. Resharding progress percentage (P1, 3-4d)
2. Estimated remaining work based on observed throughput (P1, 3-4d)
3. Copy throughput metrics (P1, 3-4d)
4. Pause and resume support (P1, 5-7d, complex)
5. Destination schema compatibility validation (P1, 5-7d)
6. Data checksum verification (P1, 5-7d)
7. Cutover readiness validation (P1, 5-7d)
8. Detailed resharding reports (P2, 3-4d)
9. Reverse migration planning primitives (P2, 3-4d)

**Total:** ~33-41 days

---

## 1. Resharding Progress Percentage

**Priority:** P1 | **Effort:** 3-4 days | **Files:** `src/resharding/resharding-tracker.ts`, `resharding-types.ts`

### Purpose
Calculate and expose real-time percentage of work completed during resharding across multiple dimensions: units, rows, bytes, and time.

### Implementation

#### Key Components
1. **ReshardingProgress interface** - Defines progress metrics
2. **ReshardingProgressCalculator class** - Calculates progress from status info
3. **Dimension tracking** - Units, rows, bytes with weighted averages

#### ProgressCalculator Features
- Calculate percentage for each dimension
- Support weighted overall percentage
- Handle edge cases (0 total, missing data)
- Estimate bytes from rows

#### Integration Points
- `ReshardingManager.getProgress()` - Get progress for active migration
- `ReshardingManager.getMigrationProgress(migrationId)` - Get progress for specific migration
- Progress events via `onProgress` hooks

### Acceptance Criteria
- [ ] Progress calculation for all dimensions
- [ ] Weighted overall percentage
- [ ] API methods for progress retrieval
- [ ] Progress events emitted
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 2. Estimated Remaining Work Based on Observed Throughput

**Priority:** P1 | **Effort:** 3-4 days | **Files:** `src/resharding/resharding-tracker.ts`

### Purpose
Calculate estimated time remaining using sliding window throughput statistics.

### Implementation

#### Key Components
1. **ThroughputTracker class** - Tracks copy rates over time
2. **Sliding window statistics** - Maintains recent samples
3. **Time remaining estimation** - Projects completion based on current rates
4. **Confidence calculation** - Rates confidence based on sample quality

#### ThroughputTracker Features
- Record samples periodically
- Calculate current and average rates (rows/sec, bytes/sec, units/sec)
- Estimate time remaining for each dimension
- Use maximum estimate across dimensions
- Confidence scoring based on sample count

#### Integration Points
- Integrated with ProgressCalculator
- Automatic sampling during copy operations
- Exposed via progress API

### Acceptance Criteria
- [ ] Sliding window throughput tracking
- [ ] Time remaining estimation
- [ ] Confidence calculation
- [ ] Integration with progress tracking
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 3. Copy Throughput Metrics

**Priority:** P1 | **Effort:** 3-4 days | **Files:** `src/resharding/resharding-metrics.ts`, `resharding-types.ts`

### Purpose
Provide comprehensive metrics about copy operations including rates, latencies, and cumulative statistics.

### Implementation

#### Key Components
1. **CopyMetrics interface** - Defines all metrics
2. **CopyMetricsTracker class** - Tracks and calculates metrics
3. **Rate calculations** - Rows/sec, bytes/sec, units/sec, batches/sec
4. **Latency percentiles** - p50, p95, p99
5. **Error tracking** - Total, by type, retry count
6. **Current batch tracking** - Real-time batch info

#### MetricsTracker Features
- Start/record/complete batch tracking
- Rate calculations using sliding window
- Latency histogram for percentiles
- Error categorization
- Current batch duration and size

#### Integration Points
- `ReshardingCopier` - Track each batch
- `ReshardingManager.getCopyMetrics()` - Retrieve metrics
- `getAllCopyMetrics()` - All migrations
- Metrics events via hooks

### Acceptance Criteria
- [ ] Comprehensive metrics tracking
- [ ] Rate calculations
- [ ] Latency percentiles
- [ ] Error tracking
- [ ] Batch statistics
- [ ] API methods
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 4. Pause and Resume Support

**Priority:** P1 | **Effort:** 5-7 days | **Files:** `src/resharding/resharding-manager.ts`, `resharding-copier.ts`, `resharding-change-capture.ts`, `resharding-store.ts`

### Purpose
Allow operators to pause resharding operations for maintenance, troubleshooting, or resource management, then resume from where it left off.

### Implementation

#### Key Components
1. **Pause/Resume types** - Options, results, info interfaces
2. **ReshardingManager.pause()** - Pause current migration
3. **ReshardingManager.resume()** - Resume paused migration
4. **ReshardingCopier pause support** - Stop copy operations
5. **ReshardingChangeCapture pause support** - Stop CDC
6. **State management** - Store pause state and reason

#### Pause Flow
1. Validate transition to paused state
2. Wait for current batch to complete (configurable)
3. Acquire worker lock
4. Transition status to 'paused'
5. Pause copier and change capture
6. Update paused timestamp

#### Resume Flow
1. Validate transition from paused state
2. Determine target status (copying or catching_up)
3. Acquire worker lock
4. Transition status from paused
5. Resume copier and change capture
6. Verify checkpoint resumability

#### Pre-Resume Validation
- Check shard availability
- Validate checkpoint state
- Verify schema compatibility
- Check for active transactions

### Acceptance Criteria
- [ ] Pause method with options (wait for completion, timeout)
- [ ] Resume method with validation
- [ ] Pause info API
- [ ] State transitions
- [ ] Graceful pause (wait for batch completion)
- [ ] Pre-resume validation
- [ ] Integration with copier and CDC
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 5. Destination Schema Compatibility Validation

**Priority:** P1 | **Effort:** 5-7 days | **Files:** `src/resharding/resharding-validator.ts`, `resharding-types.ts`

### Purpose
Validate that destination schemas are compatible with source schemas before and during migration to prevent data loss or corruption.

### Implementation

#### Key Components
1. **Schema compatibility types** - Status, issues, report interfaces
2. **SchemaIntrospector class** - Extract schema from PostgreSQL
3. **SchemaComparator class** - Compare schemas
4. **SchemaValidator class** - Orchestrate validation

#### Schema Introspection
- Table schema extraction
- Column definitions (name, type, nullable, default, etc.)
- Constraints (PK, FK, unique, check)
- Indexes
- Triggers
- Sequences
- Ownership and comments

#### Schema Comparison
- Column presence and properties
- Data type compatibility matrix
- Constraint comparison (PK, FK, unique, check)
- Index comparison (unique indexes)
- Column order validation
- Default value validation

#### Validation Features
- Per-table validation
- Configurable validation options (strict mode, which checks to run)
- Issue severity levels (error, warning, info)
- Suggestions for fixes

#### Integration Points
- Pre-migration validation
- `ReshardingManager.validateDestinationSchema()`
- `isDestinationSchemaCompatible()`
- Integration with start() method

### Acceptance Criteria
- [ ] Complete schema introspection
- [ ] Data type compatibility checking
- [ ] Constraint comparison
- [ ] Index comparison
- [ ] Comprehensive issue reporting
- [ ] Severity levels
- [ ] Pre-migration validation
- [ ] API methods
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 6. Data Checksum Verification

**Priority:** P1 | **Effort:** 5-7 days | **Files:** `src/resharding/resharding-verifier.ts`, `resharding-types.ts`

### Purpose
Verify data integrity by comparing checksums of source and destination data.

### Implementation

#### Key Components
1. **Checksum types** - Algorithm, result, comparison, report interfaces
2. **ChecksumCalculator class** - Calculate checksums for data
3. **ChecksumVerifier class** - Verify source vs destination

#### ChecksumCalculator Features
- Multiple algorithms (md5, sha256, sha512)
- Batch processing for large tables
- Keyset-based pagination
- Consistent serialization for checksum calculation
- Configurable batch size

#### ChecksumVerifier Features
- Unit-level verification
- Table-level verification
- Migration-level verification
- Incremental verification (for ongoing migration)
- Error handling and reporting

#### Verification Process
1. Calculate source checksum for unit/table
2. Calculate destination checksum
3. Compare checksums
4. Report results with details

#### Integration Points
- Integration with validation
- `ReshardingManager.verifyChecksums()`
- `verifyUnitChecksums()`
- Integration with cutover readiness

### Acceptance Criteria
- [ ] Multiple checksum algorithms
- [ ] Batch processing
- [ ] Unit-level verification
- [ ] Migration-level verification
- [ ] Integration with validation
- [ ] API methods
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 7. Cutover Readiness Validation

**Priority:** P1 | **Effort:** 5-7 days | **Files:** `src/resharding/resharding-cutover-validator.ts`, `resharding-types.ts`

### Purpose
Validate all prerequisites are met before allowing cutover to ensure a smooth transition.

### Implementation

#### Key Components
1. **Readiness check types** - Individual check definitions
2. **CutoverReadinessValidator class** - Run all checks
3. **Readiness report** - Aggregated results

#### Readiness Checks
1. **Status check** - Verify migration is in ready_for_cutover state
2. **Data copy completeness** - All units, rows, and change log lag
3. **Validation passed** - All validation reports show success
4. **Change log drained** - No unapplied change events
5. **Shard health** - All source and target shards are healthy
6. **Write fence** - Write fences are in place
7. **No active transactions** - No in-progress transactions on source shards

#### Check Features
- Individual check execution
- Configurable check options (skip, timeout, strict mode)
- Detailed check results with metrics
- Confidence levels
- Error and warning messages

#### Integration Points
- Pre-cutover validation
- `ReshardingManager.checkCutoverReadiness()`
- Integration with cutover method
- Block cutover if not ready

### Acceptance Criteria
- [ ] All 7 readiness checks
- [ ] Configurable options
- [ ] Detailed reporting
- [ ] Confidence scoring
- [ ] Integration with cutover
- [ ] API methods
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 8. Detailed Resharding Reports

**Priority:** P2 | **Effort:** 3-4 days | **Files:** `src/resharding/resharding-reporter.ts`, `resharding-types.ts`

### Purpose
Generate comprehensive reports about resharding operations in multiple formats.

### Implementation

#### Key Components
1. **Report types** - Report structure and format options
2. **ReshardingReporter class** - Generate reports
3. **Multiple format support** - Markdown, JSON, HTML

#### Report Sections
1. **Metadata** - Migration ID, generated at, format, version
2. **Summary** - Status, progress, time remaining, timestamps, duration
3. **Migration details** - Topologies, strategy, tables, estimated totals
4. **Progress** - Units, rows, bytes (total, completed, failed, remaining)
5. **Performance** - Throughput, copy metrics, batch statistics
6. **Issues** - Errors, warnings, validation failures, checksum mismatches
7. **Units** - By status, by shard pair
8. **Recommendations** - Immediate actions, warnings, suggestions

#### Reporter Features
- Data collection from manager
- Progress calculation
- Throughput estimation
- Issue aggregation
- Recommendation generation
- Format conversion (markdown, JSON, HTML)
- Custom CSS for HTML reports

#### Integration Points
- `ReshardingManager.generateReport()`
- `generateFormattedReport()`
- `saveReportToFile()`
- Report for current or specific migration

### Acceptance Criteria
- [ ] All report sections
- [ ] Multiple format support
- [ ] Data collection from manager
- [ ] Recommendation generation
- [ ] File export
- [ ] API methods
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 9. Reverse Migration Planning Primitives

**Priority:** P2 | **Effort:** 3-4 days | **Files:** `src/resharding/resharding-reverse.ts`, `resharding-types.ts`

### Purpose
Provide primitives for planning and executing reverse migrations (rolling back to original shards).

### Implementation

#### Key Components
1. **Reverse migration types** - Plan, unit, dependency, risk, step interfaces
2. **ReverseMigrationPlanner class** - Generate reverse migration plans

#### Reverse Migration Plan
- Swaps source and target topologies
- Reverses all migration units
- Identifies dependencies
- Identifies risks with mitigations
- Generates execution steps
- Estimates durations

#### Planner Features
- **Plan generation** - Create reverse migration plan from original
- **Conflict checking** - Detect conflicts with other migrations
- **Risk identification** - Data loss, downtime, schema changes, etc.
- **Step generation** - Setup, copy, validate, cutover, cleanup
- **Rollback plan** - Generate rollback for reverse migration
- **Dependency tracking** - Other migrations that might conflict

#### Reverse Migration Risks
1. Data loss risk
2. Downtime risk
3. Schema change risk
4. Data growth risk
5. Missing data risk
6. Concurrent modifications risk

#### Integration Points
- `ReshardingManager.planReverseMigration()`
- `checkReverseMigrationConflicts()`
- `generateReverseMigrationRollbackPlan()`
- `startReverseMigration()`

### Acceptance Criteria
- [ ] Reverse migration plan generation
- [ ] Unit reversal
- [ ] Dependency identification
- [ ] Risk identification with mitigations
- [ ] Step generation
- [ ] Conflict checking
- [ ] Rollback plan generation
- [ ] API methods
- [ ] Unit tests (100%)
- [ ] Documentation

---

## 📅 Timeline

| Week | Features | Status |
|------|----------|--------|
| 15 | #1 Progress %, #2 Estimated Remaining, #3 Copy Metrics | Not Started |
| 16 | #4 Pause/Resume, #5 Schema Validation | Not Started |
| 17 | #6 Checksum Verification, #7 Cutover Readiness | Not Started |
| 18 | #8 Detailed Reports, #9 Reverse Migration | Not Started |

---

## ✅ Success Criteria

### Phase Completion
- ✅ All 9 features implemented
- ✅ 100% unit test coverage for new code
- ✅ Integration tests passing
- ✅ Documentation updated
- ✅ Code review completed
- ✅ No breaking changes

### Feature Quality
- ✅ Progress tracking accurate and real-time
- ✅ Throughput estimation reliable
- ✅ Pause/resume works correctly
- ✅ Schema validation comprehensive
- ✅ Checksum verification catches discrepancies
- ✅ Cutover readiness prevents unsafe cutovers
- ✅ Reports comprehensive and useful
- ✅ Reverse migration planning complete

---

## 🎯 Dependencies

### Between Features
- Progress % (#1) is foundation for Estimated Remaining (#2) and Reports (#8)
- Throughput Metrics (#3) feeds into Progress % (#1) and Estimated Remaining (#2)
- Schema Validation (#5) and Checksum (#6) feed into Cutover Readiness (#7)
- All features feed into Reports (#8)

### External Dependencies
- PostgreSQL connection pools
- ShardManager access
- ReshardingStore access
- ReshardingCopier access
- ReshardingChangeCapture access

---

## 📚 References

- [working-issues.md](./working-issues.md) - Feature tracking
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Technical architecture
- [PLAN.md](./PLAN.md) - Overall implementation plan
- [README.md](./README.md) - Usage documentation
- [src/resharding/](../src/resharding/) - Existing resharding code

---

## 💡 Implementation Notes

### Design Principles
1. **Non-breaking changes** - All new features should be additive
2. **Backward compatibility** - Existing functionality must continue to work
3. **Testability** - Each component should be independently testable
4. **Observability** - All operations should emit appropriate events and logs
5. **Configurability** - Features should be configurable via options

### Testing Strategy
- Unit tests for each class and method
- Integration tests for component interactions
- End-to-end tests for complete workflows
- Performance tests for metrics and progress tracking
- Error handling and edge case tests

### Documentation Requirements
- API documentation for new methods
- Usage examples in README
- Migration guides for new features
- Performance considerations
- Limitations and caveats

---

## 🚀 Next Steps

1. **Review and approve** this implementation plan
2. **Prioritize features** based on business needs
3. **Assign features** to development teams
4. **Create tickets** for each feature
5. **Begin implementation** with feature #1 (Progress Percentage)
6. **Write tests** concurrently with implementation
7. **Update documentation** as features are completed
