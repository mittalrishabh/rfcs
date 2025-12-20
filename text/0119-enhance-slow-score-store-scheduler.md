# Enhance Slow Store Scheduler

- RFC PR: https://github.com/tikv/rfcs/pull/119
- Track issue: https://github.com/tikv/pd/issues/9359

## Summary

Added network status check in [Slow Store Scheduler](https://github.com/tikv/pd/blob/master/pkg/schedule/schedulers/evict_slow_store.go).

## Motivation

Assume a store in the cluster is experiencing network jitter. This jitter causes region leaders in the store to be re-campaigned by raft on other stores, resulting in leader transfers. However, PD is unable to detect the network jitter on the TiKV store, because PD can recieve the store heartbeat in this network status. Consequently, the PD's balance leader scheduler schedules the leader back to the problematic TiKV, leading to increased latency in the cluster.

To address the above problem, we need to add a new network status feedback mechanism to PD, and avoid to transfer leaders to the problematic TiKV node. And, when the network is really bad, we will also try to evict the leader directly.

## Detailed design

### Slow Store Scheduler

Slow store scheduler is an existed scheduler in PD. Its current function is to detect whether the TiKV disk fails and quickly schedule the leaders away. Here is its steps:

1. Each TiKV collects the timeout I/O requests locally (during each inspect-interval)
2. Count `TimeoutRatio` and `SlowScore`
3. Send `SlowScore` to PD by store heartbeat
4. [pd side] If `SlowScore` >= 100, evict the leaders from this TiKV store.
5. [pd side] If `SlowScore` <= 1, recover this store
6. If `SlowScore` >= 100, a store heartbeat will be trigger at once to tell PD the store is slow.

In this RFC, we will enhance its detection of TiKV network and continue to use its algorithm for calculating slow scores. 

### NetworkSlowScore

`NetworkSlowScore` is a score that measures the network status of one store. The larger it is, the more likely there is a problem with the network. The value range of `NetworkSlowScore` is [1,100].

`NetworkSlowScore` is a score that rises quickly during a failure and falls smoothly after recovery. This allows PD to react quickly to continuous failures and allow tikv to resume its functionality after recovery.

#### How to count

When there is a problem with the network, the request sent by the store to other nodes(contain PD and TiKV) will time out, so we choose the request timeout rate as an indicator to measure the network status.

```go
init NetworkSlowScore = 1
for each NETWORK_ROUND_TICKS * InspectInterval:
    TimeoutRatio = TimeoutTicks/NETWORK_ROUND_TICKS
    if TimeoutRatio < NETWORK_TIMEOUT_RATIO_THRESHOLD:
        NetworkSlowScore = NetworkSlowScore - (100 * (InspectInterval / NETWORK_RECOVERY_INTERVALS))
    else:
        // Normalize time out ratio
        normalizedTimeoutRatio = min(TimeoutRatio, RatioMaxThresh) / RatioMaxThresh
        // RatioMaxThresh: The maximal tolerated timeout ratio.
        NetworkSlowScore = min(100, NetworkSlowScore * (1 + normalizedTimeoutRatio))
```

#### Score increase

When some requests times out, the `NetworkSlowScore` starts to rise. When the `NetworkSlowScore` reaches 100, PD will think that there is a network problem with the tikv.
1. **RatioMaxThresh** is a maximum tolerance threshold for the timeout ratio. When it exceeds this value, we consider the store unavailable.
    - For disk io slow store, RatioMaxThresh was set to 0.1
    - For network slow store, RatioMaxThresh will be set to 1.0
2. Next, the timeout rate is normalized.
    $$normalizedTimeoutRatio = min(TimeoutRatio, RatioMaxThresh) / RatioMaxThresh$$
3. Finally, `NetworkSlowScore` is enlarged according to the following formula
    $$NetworkSlowScore = min(100, NetworkSlowScore * (1 + normalizedTimeoutRatio))$$

#### Score decay

100 to 1 is a linear decrease over time. If the store's network is restored, we assume it will complete this process within **MinTTR**. Therefore, the formula for calculating the NetworkSlowScore is 
$$NetworkSlowScore = NetworkSlowScore - (100 * (InspectInterval / MinTTR))$$

- For disk io slow store, MinTTR is set to 5 min.
- For network slow store, MinTTR will be set to 10 min.

### How to collect TimeoutRatio

There are two ways to collect TimeoutRatio:

- Create a PD <-> TiKV health check mechanism. Directly use the information obtained by PD to determine the network status of TiKV. 
- Establish TiKV <-> TiKV health check mechanism. Each TiKV still uploads the health information obtained by the health check through store heartbeat.

#### PD <-> TiKV health check

PD <-> TiKV health check is equivalent to a higher-frequency store heartbeat. However, it is only responsible for health checks and does not mix other functions.

PD <-> TiKV health check is a very simple framework. However, it can't handle this situation that the problematic tikv is only experiencing network problems with other tikv nodes, but the network is normal with the pd nodes.

#### TiKV <-> TiKV health check

When a tikv has a network problem, other tikvs will also experience request timeouts when accessing the problematic tikv. In this case, the slow score of the normal tikv may also increase, and if it lasts for a long time, it may even reach 100, triggering the slow store mechanism.

To avoid this problem, we can send the calculated network slow score and its corresponding store_id to PD. If the `NetworkSlowScore` of a node equals to 1, it is considered that the network of the node is normal, and not be sent to PD to reduce the traffic packet size.

#### Summary

~~Since the first method can quickly and effectively solve most scenarios, we will deliver the first method now and then complete the second method.~~

In the latest implementation, we completed the second method.

### HealthChecker

It introduces a dedicated multi-threaded `HealthChecker` that probes every store once per `InspectInterval`. Therefore, each time the SlowScore is computed, it can directly fetch the latest network latency information from the `HealthChecker` .

### PD scheduler

#### Stop balance leader

Because network status between nodes is bidirectional, if nodes A and B experience jitter, both $Score_{AB}$ and $Score_{BA}$ rise. You must examine scores involving other nodes to pinpoint the actual slow node.

The pseudo code is as follows:

```
potential = set()
for src in stores:
    for dst in stores:
        if src == dst:
            continue
        if networkSlowStore[src][dst] > networkSlowStoreIssueThreshold:
            potential.update([src, dst])

realSlowStores = set()
for suspect in potential:
    fluctuation = 0
    for target, score in networkSlowScores[suspect]:
        if target in potential:
            continue  # ignore suspect-to-suspect links
        if score >= networkSlowStoreFluctuationThreshold:  # e.g. 10
            fluctuation += 1
    if fluctuation >= 2:
        realSlowStores.add(suspect)  # stop balancing leaders back here
```

#### Evict leader

If a store’s network keeps deteriorating, it can poison the entire cluster. Raft alone would need a long time to re-elect every region leader, so we proactively evict leaders.

On each store heartbeat, compute that store’s average `NetworkSlowScore` toward all other stores. Whenever `AvgNetworkSlowScore ≥ 99`, increment a counter. Once the counter reaches 6, evict leaders from that store.
> Why six heartbeats? Each store heartbeat arrives every 10s, so seeing an average score above 99 across six consecutive heartbeats proves the store suffered jitter throughout that full minute.

### Related Configuration

#### Exported

`inspect-network-interval`:
- Type: Integer
- Default value: 100
- Unit: ms
- Range: 0 ∪ [10, +∞)
- This parameter is used to set the inspect interval in the health check. Zero represents network inspect is disabled. It also represents the sensitivity and growth rate. When the `inspect-network-interval` is smaller, the more data is detected per unit time, and the score is likely to grow faster.

`recovery-duration`
- Type: Integer
- Default value: 1800
- Unit: s
- Range: [0, +∞)
- This parameter is used to set the slow store recovery duration. It already exists in PD, and in the future it will jointly control the recovery duration of disk and network.

`enable-network-slow-store`
- Type: Bool
- Default value: false
- This is a switch to control network-slow-store-scheduler

`inspect-network-interval` is a parameter in tikv-server, which is similar with [`inspect-interval`](https://docs.pingcap.com/tidb/dev/tikv-configuration-file/#inspect-interval). And `recovery-duration` and `enable-network-slow-store` can be set via `pd-ctl scheduler config evict-slow-store-scheduler set`.

#### Internal

- `NETWORK_ROUND_TICKS`: 3. This means `NetworkSlowScore` is recalculated after every three `inspect-network-interval` cycles.
- `NETWORK_TIMEOUT_RATIO_THRESHOLD`: 1.0. See the formulas in the "Score increase" section for how it’s used.
- `NETWORK_TIMEOUT_THRESHOLD`: 1. Unit: sec. Any probe taking longer than 1 sec is treated as a timeout.

