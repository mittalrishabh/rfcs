# RFC: Region-Level Resource Isolation in TiKV

## Summary

This RFC proposes enhancements to TiKV's resource control system to provide region-level isolation, preventing hot regions from overwhelming tenant resources. The design extends the existing resource group-based priority system with region-level RU (Resource Unit) tracking and introduces traffic moderation mechanisms.

## Motivation

### Current State

TiKV implements resource control at the **resource group level**:
- Resource groups represent tenants and track RU (Request Unit) consumption
- Each resource group has a `group_priority` (LOW, MEDIUM, HIGH) configured in PD
- The `ResourceController` maintains virtual time (VT) per resource group for fairness. VT increases as a group consumes resources - groups with higher VT have consumed more and get lower scheduling priority
- The DM (Deadline Multilevel) queue orders tasks by priority: `concat_priority_vt(group_priority, resource_group_virtual_time)`. Lower values are scheduled first, so high-priority groups with low VT run first
- VT is periodically normalized to prevent starvation: lagging groups (low VT) are pulled toward the leader (highest VT), and all VTs are reset when nearing overflow
- The unified read pool uses yatp's priority queue which implements this DM queue using a SkipMap

## Goals
These functionalities are missing in the current state. 

1. **Region-level fairness**: Hot regions (with hot keys or large scans) should be deprioritized to prevent resource monopolization within a tenant
3. **Traffic Moderation**: In a multi-tenant SOA environment, setting correct rate limits is challenging - limits that are too tight reject valid traffic, while limits that are too loose allow overload. Instead of hard rate limits, implement adaptive traffic moderation that responds to sudden spikes on hot regions by gracefully deprioritizing rather than outright rejecting requests
4. **Queue Fairness**: Ensure the unified read pool queue maintains fairness across tenants/regions/background traffic. In the existing system any one tenant or background traffic can consume the entire queue. 

## Design

### Overview

Introduce **region-level virtual time (VT)** alongside existing **resource group VT**. Each request's priority is determined by three factors in hierarchical order:

1. **group_priority**: Tenant priority (HIGH/MEDIUM/LOW) - tenant isolation
2. **group_vt**: Resource group virtual time - tenant fairness
3. **region_vt**: Region virtual time - region fairness

### Priority Structure

Replace the current 64-bit `u64` priority with a struct:

```rust
struct TaskPriority {
    group_priority: u8,   // 1-16 (HIGH/MEDIUM/LOW from PD)
    group_vt: u64,        // Resource group virtual time
    region_vt: u64,       // Region virtual time
}
```

**Comparison order** (most significant first):
1. `group_priority`: Higher value = higher priority (tenant isolation)
2. `group_vt`: Lower value = higher priority (tenant fairness)
3. `region_vt`: Lower value = higher priority (region fairness)

**On task scheduling**:
- Group VT increases by `vt_delta_for_get` (fixed per group)
- Region VT increases by `vt_delta_for_get` (varies based on region hotness)
- Hot regions accumulate VT faster → pushed back in queue

**On task completion**:
- Group VT increases by actual CPU time consumed
- Region VT increases by actual CPU time consumed

**Periodic normalization** (every ~1 second):
- Find min/max VT across all groups/regions
- Pull lagging entities toward leader (prevent starvation)
- Reset all VTs if near overflow

### Traffic moderation and split/scatter 

Currently, split/scatter is non-deterministic when node is overloaded - it depends on how many requests on this region are succeeded. With this design, Hot regions accumulate high VT and get deprioritized, which slows down split decisions based on served QPS. When the region eventually splits, the new regions can either:
1. **Inherit parent VT** (recommended): New regions start with the same high VT as parent, maintaining deprioritization until they cool down naturally via normalization
2. **Reset to zero VT**: New regions start fresh, immediately getting normal priority

Option 1 combined with delayed splitting provides stronger traffic moderation - the system temporarily reduces throughput to the hot table/region until the load naturally subsides or spreads across the split regions

### Background Task Demotion

Background tasks (GC, compaction, statistics) use LOW `group_priority` regardless of their resource group's configured priority:
```
group_priority = LOW  // instead of resource group's configured priority
```

This ensures foreground traffic is always prioritized over background.

### Queue Eviction

When queue is full:
1. Calculate priority of incoming task
2. Compare with lowest priority task in queue
3. If incoming has higher priority: evict lowest, enqueue incoming
4. Else: reject incoming with ServerIsBusy

Evicted tasks are failed with ServerIsBusy error.

## Implementation

### 1. yatp Modifications

**Change priority type from `u64` to struct**:

```rust
// In yatp/src/queue/priority.rs

struct TaskPriority {
    group_priority: u8,
    group_vt: u64,
    region_vt: u64,
}

// Implement Ord with hierarchical comparison
// Update TaskPriorityProvider trait
trait TaskPriorityProvider {
    fn priority_of(&self, extras: &Extras) -> TaskPriority;
}

// Update MapKey
struct MapKey {
    priority: TaskPriority,
    sequence: u64,
}
```

### 2. TaskMetadata Changes

Add region_id and is_background fields:

```rust
// In components/tikv_util/src/resource_control.rs

const REGION_ID_MASK: u8 = 0b0000_0100;
const IS_BACKGROUND_MASK: u8 = 0b0000_1000;

impl TaskMetadata {
    fn region_id(&self) -> u64 {
        // Extract from metadata bytes
    }

    fn is_background(&self) -> bool {
        self.mask & IS_BACKGROUND_MASK != 0
    }
}
```

### 3. Region VT Tracker

Create new component for region-level tracking:

```rust
// In components/resource_control/src/region_tracker.rs

struct RegionResourceTracker {
    region_vts: DashMap<u64, RegionVtTracker>,
}

struct RegionVtTracker {
    virtual_time: AtomicU64,
    vt_delta_for_get: AtomicU64,
}

impl RegionResourceTracker {
    fn get_and_increment_vt(region_id) -> u64 {
        // Similar to ResourceGroup::get_priority()
        // Returns current VT and increments by vt_delta
    }

    fn update_vt_deltas() {
        // Periodically adjust vt_delta based on region hotness
        // ratio = region_ru / avg_ru
        // delta = base_delta * ratio
    }

    fn normalize_region_vts() {
        // Periodically normalize VTs (like update_min_virtual_time)
        // Pull lagging regions forward, reset if near overflow
    }

    fn consume(region_id, cpu_time, keys, bytes) {
        // Update EMA metrics
        // Increment VT based on actual consumption
    }
}
```

### 4. Priority Calculation

Update ResourceController to return TaskPriority:

```rust
// In components/resource_control/src/resource_group.rs

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &Extras) -> TaskPriority {
        let metadata = TaskMetadata::from(extras.metadata());

        // 1. Get group VT
        let group_vt = self.resource_group(metadata.group_name())
            .get_group_vt(level, override_priority);

        // 2. Get region VT
        let region_id = metadata.region_id();
        let region_vt = self.region_tracker.get_and_increment_vt(region_id);

        // 3. Use LOW priority for background tasks
        let group_priority = if metadata.is_background() {
            LOW
        } else {
            base_priority  // from resource group config
        };

        TaskPriority { group_priority, group_vt, region_vt }
    }

    fn approximate_priority_of(&self, extras: &Extras) -> TaskPriority {
        // Read VT without incrementing (for eviction check)
    }
}
```

### 5. Queue Eviction

Extend yatp to support eviction:

```rust
// In yatp/src/queue/priority.rs

impl QueueCore {
    fn try_evict_for_priority(incoming_priority: TaskPriority) -> bool {
        if let Some(lowest_entry) = self.pq.back() {
            if incoming_priority < lowest_entry.priority {
                // Evict lowest priority task
                if let Some(entry) = self.pq.pop_back() {
                    // Send eviction signal via oneshot channel
                    entry.eviction_handle.evict();
                    return true;
                }
            }
        }
        false
    }
}
```

Wrap futures with eviction notification:

```rust
// In src/read_pool.rs

struct EvictableFuture<F> {
    future: F,
    eviction_rx: oneshot::Receiver<()>,
}

// On eviction: send signal via oneshot channel
// Future polls eviction_rx and returns ServerIsBusy if signaled
```

Update ReadPoolHandle::spawn():

```rust
impl ReadPoolHandle {
    fn spawn(...) -> Result<(), ReadPoolError> {
        // 1. Calculate approximate priority (without VT increment)
        let approx_priority = resource_ctl.approximate_priority_of(&extras);

        // 2. Check queue full
        if running_tasks >= max_tasks {
            // 3. Try eviction
            if !remote.try_evict_for_priority(approx_priority) {
                return Err(ReadPoolError::UnifiedReadPoolFull);
            }
        }

        // 4. Spawn task (actual priority calculated by yatp)
        remote.spawn(task_cell);
    }
}
```

### 6. Tracking Integration

Wire region tracking into execution paths:

```rust
// In src/storage/mod.rs and src/coprocessor/

// After task completes:
region_tracker.consume(
    region_id,
    cpu_time,
    keys_scanned,
    bytes_read,
);
```

### 7. Background Task

Periodic normalization and delta updates:

```rust
// Run every 1 second
fn periodic_region_maintenance() {
    region_tracker.normalize_region_vts();
    region_tracker.update_vt_deltas();
}
```

## Configuration

New configuration options in `tikv.toml`:

```toml
[resource-control]
# Enable region-level resource tracking
enable-region-tracking = true
```

## Drawbacks
1. Currently, split/scatter is non-deterministic when node is overloaded - it depends on how many requests on this region are succeeded. With this design, hot regions are deliberately deprioritized, so split/scatter may take longer when overloaded. This is an acceptable tradeoff for better traffic moderation
