# Spec: Advanced Consumer Patterns
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Master the transition from auto-commit to manual offset management.
- Understand the trade-offs between synchronous (`commitSync()`) and asynchronous (`commitAsync()`) offset committing, and how to combine them for optimal performance and safety.
- Implement a `ConsumerRebalanceListener` to cleanly save state or commit offsets during partition rebalances.
- Observe the internal mechanics of Consumer Groups, including standby consumers and partition assignment.

## 2. Core Concepts (CONCEPT.md)
- **Offset Management**: Auto-commit (`enable.auto.commit=true`) risks vs Manual Commit (`false`).
- **Synchronous vs Asynchronous Commits**: Blocking vs Non-blocking, retries, and the `OffsetCommitCallback`.
- **The Rebalance Protocol**: How consumers join/sync and the lifecycle of a rebalance.
- **ConsumerRebalanceListener**: `onPartitionsRevoked`, `onPartitionsAssigned`, and `onPartitionsLost`.
- **Standby Consumers**: Having more consumers than partitions in a group.

## 3. Infrastructure & Tooling
- Infrastructure Profile: **Option A: Basic KRaft Broker** (for CLI observation) & **Option C: Testcontainers** (for TDD).
- CLI Commands: 
  - `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group --describe` (to observe offsets, lag, and assignment).

## 4. Practical Implementation (README.md)
- **Step 1**: Implement a consumer with `enable.auto.commit=false` that processes batches of messages and commits offsets using `commitAsync()` for throughput, with a `commitSync()` in the `finally` block for shutdown safety.
- **Step 2**: Create a `ConsumerRebalanceListener` that flushes local state and performs a `commitSync()` in `onPartitionsRevoked()`.
- **Step 3**: Run multiple consumer instances simultaneously to force a rebalance and observe the listener triggering via logs. Use `kafka-consumer-groups.sh` to watch partitions shift.

## 5. TDD & Technical Verification
- **Test 1**: Verify At-Least-Once semantics. If an exception is thrown during processing, the offset is NOT committed.
- **Test 2**: Verify `ConsumerRebalanceListener.onPartitionsRevoked()` is executed when a consumer group rebalances using Testcontainers.
- **Test 3**: Verify `CommitFailedException` handling.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: Strictly **At-Least-Once**. Messages are processed *before* offsets are committed.
- **Resilience**: The lab directly addresses the chaos of consumer rebalances. By hooking into `ConsumerRebalanceListener`, we ensure no duplicate processing or lost state during dynamic scaling up/down. We also handle the `CommitFailedException` scenario.

## 7. Self-Assessment Questions
1. Why is `commitAsync()` generally preferred over `commitSync()` in the main `poll()` loop, and when is `commitSync()` absolutely necessary?
2. What happens if a rebalance occurs before you have committed the offsets for the currently processing batch?
3. What is the difference between `onPartitionsRevoked()` and `onPartitionsLost()`?
