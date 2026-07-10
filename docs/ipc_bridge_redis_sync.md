# IPC Bridge Redis Key Sync

`ipc_bridge` can periodically mirror explicitly configured Redis Hash or String keys over the same ZMQ transport used by IPC routes. It does not scan Redis and does not use Redis `DUMP`/`RESTORE`.

For a Hash, the source runs `HGETALL`. The receiver atomically replaces the destination key with `MULTI`, `DEL`, `HSET`, `EXEC`, so removed fields are also removed remotely. For a String, the source uses `GET` and the receiver uses `SET`. A missing source key deletes the configured destination key.

## Sender

Add one route per model output Hash. Multiple routes may share the same remote ZMQ address.

```yaml
routes:
  - id: redis_model_thresholds_mid_chg_30s
    from:
      type: redis
      endpoint: "model_score_rolling_thresholds_<model_name>"
      redis_type: hash
      poll_interval_ms: 5000
      redis:
        host: 127.0.0.1
        port: 6379
        db: 0
    to:
      type: zmq
      endpoint: "tcp://<remote-host>:6360"
```

## Receiver

The route id must match the sender. Redis credentials, DB, prefix, and destination key may differ.

```yaml
routes:
  - id: redis_model_thresholds_mid_chg_30s
    from:
      type: zmq
      endpoint: "tcp://0.0.0.0:6360"
    to:
      type: redis
      endpoint: "model_score_rolling_thresholds_<model_name>"
      redis_type: hash
      redis:
        host: 127.0.0.1
        port: 6379
        db: 0
```

The receiver key is an authoritative mirror. Do not have another process write the same destination key. Expiry/TTL metadata is not synchronized; model score threshold Hash keys are persistent, so this does not affect the current model pub use case.
