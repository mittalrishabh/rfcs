# Design Doc: Region-Level Resource Isolation

## Summary

This document proposes region-level isolation in TiKV, preventing hot regions from overwhelming tenant resources. The design extends the existing resource group-based priority system with region-level virtual time (VT) tracking.

## Motivation

### Current State

TiKV implements resource control at the **resource group level**:
- Resource groups represent tenants and track RU (Resource Unit) consumption
- Each resource group has a `group_priority` (LOW, MEDIUM, HIGH)
- The `ResourceController` uses mClock algorithm with virtual time (VT) per resource group
- Tasks are ordered by: `concat_priority_vt(group_priority, resource_group_vt)`

### Problem

**No region-level fairness**: A hot region (hot keys or large scans) can monopolize resources within a tenant, starving other regions belonging to the same tenant.

### Goal

Hot regions should be deprioritized to prevent resource monopolization within a tenant.

## Design

### Overview

Introduce **region-level virtual time (VT)** alongside existing **resource group VT**. Each request's priority is determined by three factors in hierarchical order:

1. **group_priority**: Tenant priority (HIGH/MEDIUM/LOW) - tenant isolation
2. **group_vt**: Resource group virtual time - tenant fairness
3. **region_vt**: Region virtual time - region fairness within tenant

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

### Virtual Time Updates

**On task scheduling**:
- Group VT increases by `vt_delta_for_get` (fixed per group)
- Region VT increases by `vt_delta_for_get` (varies based on region hotness)
- Hot regions accumulate VT faster → pushed back in queue

**On task completion**:
- Group VT increases by actual CPU time consumed
- Region VT increases by actual CPU time consumed

**Periodic normalization** (every ~1 second):
- Find min/max VT across all regions
- Pull lagging regions toward leader (prevent starvation)
- Reset all VTs if near overflow

### Traffic Moderation and Split/Scatter

Hot regions accumulate high VT and get deprioritized, which affects split decisions based on served QPS.

#### VT Handling for Split Regions

When a region splits, VT behavior depends on CPU utilization:

**When CPU utilization > 80% (system overloaded)**:
- Split regions share a **common VT** inherited from parent region
- Both child regions contribute to and read from the same VT tracker
- Maintains strong traffic moderation - even after splitting, the hot key/region group remains deprioritized
- Prevents split from immediately bypassing backpressure

**When CPU utilization drops < 80% (system has capacity)**:
- Split regions transition to **independent VTs**
- Each region gets its own VT tracker, initialized to common VT value
- Allows natural load balancing

**Implementation**:
- Track CPU utilization as rolling average (~10 seconds)
- On region split, create `RegionGroup` if CPU > 80%, linking child regions to shared VT
- Periodically check CPU utilization (every 1-5 seconds)
- When CPU drops < 80%, dissolve region groups and transition to independent VTs

## Implementation

### 1. Region VT Tracker

```rust
struct RegionResourceTracker {
    region_vts: DashMap<u64, RegionVtTracker>,
    cpu_utilization: AtomicU64,  // Rolling average
}

struct RegionVtTracker {
    virtual_time: AtomicU64,
    vt_delta_for_get: AtomicU64,
    parent_vt: Option<Arc<AtomicU64>>,  // Shared parent VT if CPU > 80% at split
}

impl RegionResourceTracker {
    fn get_and_increment_vt(region_id: u64) -> u64 {
        // If parent_vt exists, use shared parent VT
        // Otherwise use independent VT
    }

    fn on_region_split(parent_id: u64, child1_id: u64, child2_id: u64) {
        // Get parent VT value
        // If cpu_utilization > 80%:
        //   Create Arc<AtomicU64> with parent VT
        //   Both children share reference to parent_vt
        // Else:
        //   Both children get independent VT initialized to parent VT
    }

    fn check_and_transition_to_independent() {
        // If cpu_utilization < 80%:
        //   For each region with parent_vt:
        //     Copy parent_vt value to virtual_time
        //     Set parent_vt to None
    }

    fn update_vt_deltas() {
        // Periodically adjust vt_delta based on region hotness
        // ratio = region_ru / avg_ru
        // delta = base_delta * ratio
    }

    fn normalize_region_vts() {
        // Pull lagging regions forward, reset if near overflow
    }

    fn consume(region_id: u64, cpu_time: Duration, keys: u64, bytes: u64) {
        // Increment VT based on actual consumption
        // If parent_vt exists, increment shared parent VT
    }

    fn cleanup_inactive_regions() {
        // Remove regions with no recent VT updates
    }
}
```

### 2. TaskMetadata Changes

Add region_id field:

```rust
const REGION_ID_MASK: u8 = 0b0000_0100;

impl TaskMetadata {
    fn region_id(&self) -> u64 {
        // Extract from metadata bytes
    }
}
```

### 3. Priority Calculation

Update ResourceController to include region VT:

```rust
impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &Extras) -> TaskPriority {
        let metadata = TaskMetadata::from(extras.metadata());

        // 1. Get group VT
        let group_vt = self.resource_group(metadata.group_name())
            .get_group_vt(level, override_priority);

        // 2. Get region VT
        let region_id = metadata.region_id();
        let region_vt = self.region_tracker.get_and_increment_vt(region_id);

        TaskPriority { group_priority, group_vt, region_vt }
    }
}
```

### 4. Tracking Integration

Wire region tracking into execution paths:

```rust
// After task completes:
region_tracker.consume(
    region_id,
    cpu_time,
    keys_scanned,
    bytes_read,
);
```

### 5. Background Task

Periodic normalization and delta updates:

```rust
// Run every 1 second
fn periodic_region_maintenance() {
    region_tracker.normalize_region_vts();
    region_tracker.update_vt_deltas();
}
```

## Configuration

```toml
[resource-control]
enable-region-tracking = true
```

## Drawbacks

1. **Temporary traffic moderation**: VT-based traffic moderation is temporary. It does not persist if a node is rebooted after regions are split.

2. **Shared region fairness issues**: When multiple resource groups access the same region:
   - **Innocent tenant penalized**: Tenant A's heavy usage increases region VT, penalizing Tenant B's requests
   - **Hot region stays hot**: If tenants alternate requests, each tenant's group_vt stays low, so region never gets properly deprioritized

   Mitigation: Ensure resource groups don't share tables. Regions are generally created at table boundary if big enough.
