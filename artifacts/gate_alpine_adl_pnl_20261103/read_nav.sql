\set ON_ERROR_STOP on
BEGIN READ ONLY;
SET LOCAL statement_timeout = '20s';
WITH targets(label, target_ms, direction) AS (
    VALUES ('pre_adl', 1793355399000::bigint, 'before'),
           ('post_adl', 1793355401000::bigint, 'after'),
           ('one_hour_after_adl', 1793359000000::bigint, 'after'),
           ('pre_rehedge', 1793713512000::bigint, 'before'),
           ('latest', 9223372036854775807::bigint, 'before')
), chosen AS (
    SELECT t.label, s.snapshot_ts_ms, s.payload
    FROM targets t
    CROSS JOIN LATERAL (
        (SELECT snapshot_ts_ms, payload
         FROM strategy_snapshots
         WHERE strategy_slug = 'gate_fr_arb01' AND t.direction = 'before'
           AND snapshot_ts_ms <= t.target_ms
         ORDER BY snapshot_ts_ms DESC LIMIT 1)
        UNION ALL
        (SELECT snapshot_ts_ms, payload
         FROM strategy_snapshots
         WHERE strategy_slug = 'gate_fr_arb01' AND t.direction = 'after'
           AND snapshot_ts_ms >= t.target_ms
         ORDER BY snapshot_ts_ms LIMIT 1)
    ) s
)
SELECT json_build_object(
    'label', label, 'snapshot_ts_ms', snapshot_ts_ms,
    'snapshot_utc', to_timestamp(snapshot_ts_ms / 1000.0),
    'risk', (SELECT e->'entry' FROM jsonb_array_elements(payload->'entries') e WHERE e->>'type' = 'pre_trade_risk' LIMIT 1),
    'alpine_exposure', (SELECT r FROM jsonb_array_elements(payload->'entries') e CROSS JOIN LATERAL jsonb_array_elements(e->'entry'->'rows') r WHERE e->>'type' = 'pre_trade_exposure' AND r->>'asset' = 'ALPINE' LIMIT 1)
)
FROM chosen;
COMMIT;
