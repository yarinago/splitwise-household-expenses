# Broker Restart / Degraded Kafka

## Symptoms

- Consumer lag rises and does not drain.
- Producer logs delivery failures or timeouts.
- Consumer logs poll or commit failures.
- Kafka exporter shows under-replicated partitions or offline brokers.

## Immediate Checks

1. Check Kafka and broker pod health in the Kafka namespace.
2. Check the Strimzi `Kafka` custom resource status.
3. Check the lag dashboard for:
   - consumer lag by topic and group
   - under-replicated partitions
   - broker availability
4. Check producer and consumer logs for repeated connection, rebalance, or commit errors.

## Broker Restart Procedure

1. Confirm at least one broker remains healthy before restarting a broker pod.
2. Restart only one broker pod at a time.
3. Watch the restarted pod until it becomes Ready again.
4. Confirm partitions re-elect leaders and under-replicated partitions return to zero.
5. Confirm consumer lag starts draining again.

## If Lag Keeps Growing

1. Verify the consumer group exists and is active.
2. Confirm the consumer deployment is running with the expected `KAFKA_GROUP_ID`.
3. Confirm the consumer can still commit offsets.
4. If the consumer is unhealthy, restart the consumer deployment.
5. If the producer is flooding the topic faster than the consumer can recover, stop `loadgen` first before further debugging.

## Verification

- Topic still exists with the expected partition count.
- Consumer group is visible in Kafka exporter metrics.
- Lag decreases after broker recovery.
- The dashboard read model updates again and `/readyz` returns success.
