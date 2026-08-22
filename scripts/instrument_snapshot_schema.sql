CREATE SCHEMA IF NOT EXISTS market_metadata;

CREATE TABLE IF NOT EXISTS market_metadata.instrument_snapshot_runs (
    snapshot_id uuid PRIMARY KEY,
    schema_version integer NOT NULL,
    captured_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    effective_from timestamptz NOT NULL,
    collector_host text NOT NULL,
    collector_script_sha256 text NOT NULL CHECK (collector_script_sha256 ~ '^[0-9a-f]{64}$'),
    exchanges text[] NOT NULL,
    market_types text[] NOT NULL,
    raw_response_count integer NOT NULL CHECK (raw_response_count >= 0),
    instrument_count integer NOT NULL CHECK (instrument_count >= 0),
    manifest jsonb NOT NULL,
    inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (completed_at >= captured_at)
);

-- A scope is one complete exchange/market observation. Its interval is derived
-- from adjacent observations, so an as-of query never mixes rules from runs.
CREATE TABLE IF NOT EXISTS market_metadata.instrument_snapshot_scopes (
    snapshot_id uuid NOT NULL REFERENCES market_metadata.instrument_snapshot_runs(snapshot_id),
    exchange text NOT NULL,
    market_type text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    instrument_count integer NOT NULL CHECK (instrument_count >= 0),
    PRIMARY KEY (snapshot_id, exchange, market_type),
    UNIQUE (exchange, market_type, effective_from),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE TABLE IF NOT EXISTS market_metadata.instrument_raw_responses (
    snapshot_id uuid NOT NULL REFERENCES market_metadata.instrument_snapshot_runs(snapshot_id),
    source_id text NOT NULL,
    page integer NOT NULL CHECK (page > 0),
    exchange text NOT NULL,
    market_types text[] NOT NULL,
    request_url text NOT NULL,
    request_headers jsonb NOT NULL,
    response_headers jsonb NOT NULL,
    http_status integer NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    fetched_at timestamptz NOT NULL,
    response_sha256 text NOT NULL CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    response_body_text text NOT NULL,
    response_body jsonb NOT NULL,
    PRIMARY KEY (snapshot_id, source_id, page)
);

CREATE TABLE IF NOT EXISTS market_metadata.instrument_rules (
    snapshot_id uuid NOT NULL REFERENCES market_metadata.instrument_snapshot_runs(snapshot_id),
    captured_at timestamptz NOT NULL,
    effective_from timestamptz NOT NULL,
    exchange text NOT NULL,
    market_type text NOT NULL,
    instrument_id text NOT NULL,
    symbol text NOT NULL,
    base_asset text,
    quote_asset text,
    contract_type text,
    status text,

    price_tick_raw text,
    price_tick numeric,
    price_tick_integer numeric(78, 0),
    price_tick_scale smallint,
    price_tick_source text,

    qty_step_raw text,
    qty_step numeric,
    qty_step_integer numeric(78, 0),
    qty_step_scale smallint,
    qty_step_source text,

    min_qty_raw text,
    min_qty numeric,
    max_qty_raw text,
    max_qty numeric,
    market_qty_step_raw text,
    market_qty_step numeric,
    market_min_qty_raw text,
    market_min_qty numeric,
    market_max_qty_raw text,
    market_max_qty numeric,
    min_notional_raw text,
    min_notional numeric,
    max_notional_raw text,
    max_notional numeric,
    market_min_notional_raw text,
    market_min_notional numeric,
    market_max_notional_raw text,
    market_max_notional numeric,

    contract_multiplier_raw text,
    contract_multiplier numeric,
    contract_multiplier_components jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_id text NOT NULL,
    source_page integer NOT NULL CHECK (source_page > 0),
    rule_sha256 text NOT NULL CHECK (rule_sha256 ~ '^[0-9a-f]{64}$'),
    raw_instrument jsonb NOT NULL,
    PRIMARY KEY (snapshot_id, exchange, market_type, instrument_id),
    FOREIGN KEY (snapshot_id, source_id, source_page)
        REFERENCES market_metadata.instrument_raw_responses(snapshot_id, source_id, page),
    CHECK (price_tick IS NULL OR price_tick > 0),
    CHECK (qty_step IS NULL OR qty_step > 0),
    CHECK (min_qty IS NULL OR min_qty >= 0),
    CHECK (max_qty IS NULL OR max_qty > 0),
    CHECK (market_qty_step IS NULL OR market_qty_step > 0),
    CHECK (market_min_qty IS NULL OR market_min_qty >= 0),
    CHECK (market_max_qty IS NULL OR market_max_qty > 0),
    CHECK (min_notional IS NULL OR min_notional >= 0),
    CHECK (max_notional IS NULL OR max_notional > 0),
    CHECK (market_min_notional IS NULL OR market_min_notional >= 0),
    CHECK (market_max_notional IS NULL OR market_max_notional > 0),
    CHECK (contract_multiplier IS NULL OR contract_multiplier > 0),
    CHECK ((price_tick IS NULL) = (price_tick_raw IS NULL)),
    CHECK ((price_tick IS NULL) = (price_tick_integer IS NULL)),
    CHECK ((price_tick IS NULL) = (price_tick_scale IS NULL)),
    CHECK ((qty_step IS NULL) = (qty_step_raw IS NULL)),
    CHECK ((qty_step IS NULL) = (qty_step_integer IS NULL)),
    CHECK ((qty_step IS NULL) = (qty_step_scale IS NULL))
);

CREATE INDEX IF NOT EXISTS instrument_snapshot_scopes_asof_idx
    ON market_metadata.instrument_snapshot_scopes
    (exchange, market_type, effective_from DESC);

CREATE INDEX IF NOT EXISTS instrument_rules_symbol_idx
    ON market_metadata.instrument_rules
    (exchange, market_type, symbol, effective_from DESC);

CREATE OR REPLACE VIEW market_metadata.instrument_rule_history AS
SELECT
    rules.*,
    scopes.effective_to
FROM market_metadata.instrument_rules AS rules
JOIN market_metadata.instrument_snapshot_scopes AS scopes
  ON scopes.snapshot_id = rules.snapshot_id
 AND scopes.exchange = rules.exchange
 AND scopes.market_type = rules.market_type;

COMMENT ON TABLE market_metadata.instrument_snapshot_runs IS
    'Immutable manifests for exchange contract/instrument REST snapshots.';
COMMENT ON TABLE market_metadata.instrument_snapshot_scopes IS
    'Complete exchange/market snapshot intervals used by point-in-time backtests.';
COMMENT ON TABLE market_metadata.instrument_raw_responses IS
    'Exact public REST response text plus parsed JSON and transport metadata.';
COMMENT ON TABLE market_metadata.instrument_rules IS
    'Normalized exact-decimal order and contract rules, keyed by immutable snapshot.';
COMMENT ON VIEW market_metadata.instrument_rule_history IS
    'Point-in-time rules; use effective_from <= ts AND (effective_to IS NULL OR ts < effective_to).';
