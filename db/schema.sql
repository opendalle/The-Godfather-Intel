-- ============================================================
-- NEXUS ASIA INTEL — MASTER SCHEMA v1.0
-- The Godfather: unified supply + demand CRE intelligence
--
-- Run this in your Supabase SQL Editor (fresh database)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- for fuzzy company matching

-- ============================================================
-- COMPANIES TABLE
-- Unified entity registry — both distressed sellers + demand-side tenants
-- ============================================================
CREATE TABLE IF NOT EXISTS companies (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name                TEXT NOT NULL,
    normalized_name     TEXT,
    cin                 TEXT,                          -- MCA Corporate Identification Number
    sector              TEXT,
    industry            TEXT,
    website             TEXT,
    hq_location         TEXT,
    country             TEXT DEFAULT 'IN',
    -- Distress tracking
    risk_score          INTEGER DEFAULT 0 CHECK (risk_score BETWEEN 0 AND 100),
    distress_status     TEXT DEFAULT 'monitoring' CHECK (
        distress_status IN ('monitoring', 'watch', 'active_cirp', 'liquidation', 'resolved', 'clean')
    ),
    -- Demand tracking
    demand_score        INTEGER DEFAULT 0 CHECK (demand_score BETWEEN 0 AND 100),
    demand_status       TEXT DEFAULT 'unknown' CHECK (
        demand_status IN ('unknown', 'expanding', 'stable', 'contracting', 'new_entrant')
    ),
    -- Signal history
    first_signal_at     TIMESTAMPTZ,
    last_signal_at      TIMESTAMPTZ,
    signal_count        INTEGER DEFAULT 0,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(normalized_name)
);

CREATE INDEX IF NOT EXISTS idx_companies_name_trgm ON companies USING GIN(name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_companies_normalized ON companies(normalized_name);
CREATE INDEX IF NOT EXISTS idx_companies_risk ON companies(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_companies_demand ON companies(demand_score DESC);

-- ============================================================
-- DISTRESS EVENTS TABLE
-- Supply-side: every SARFAESI, CIRP, auction, NPA, DRT signal
-- ============================================================
CREATE TABLE IF NOT EXISTS distress_events (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_name        TEXT NOT NULL,
    company_id          UUID REFERENCES companies(id) ON DELETE SET NULL,
    signal_keyword      TEXT NOT NULL,
    signal_category     TEXT DEFAULT 'general' CHECK (
        signal_category IN (
            'insolvency', 'auction', 'restructuring', 'default',
            'legal', 'regulatory', 'general', 'sarfaesi', 'creditor_action',
            'rbi_action', 'distressed_asset', 'cirp', 'liquidation',
            'pre_leased_asset', 'cre_vacancy', 'arc_portfolio', 'pe_activity',
            'market_stress', 'financial_media', 'nclt', 'ibbi',
            'bankruptcy', 'debt_resolution', 'asset_auction', 'other'
        )
    ),
    source              TEXT NOT NULL,
    url                 TEXT,
    headline            TEXT,
    snippet             TEXT,
    -- Enrichment fields
    asset_class         TEXT CHECK (asset_class IN (
        'commercial', 'residential', 'land', 'industrial', 'hospitality', 'mixed', 'other'
    )),
    price_crore         NUMERIC(12, 2),
    location            TEXT,
    is_mmr              BOOLEAN DEFAULT FALSE,
    deal_score          INTEGER DEFAULT 0 CHECK (deal_score BETWEEN 0 AND 100),
    channel             TEXT CHECK (channel IN (
        'bank_auction', 'sarfaesi', 'drt', 'legal_intelligence',
        'pre_leased_cre', 'arc_portfolio', 'pe_activity',
        'market_distress', 'media', 'regulatory', 'other'
    )),
    order_date          DATE,
    -- Metadata
    detected_at         TIMESTAMPTZ DEFAULT NOW(),
    published_at        TIMESTAMPTZ,
    severity            TEXT DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    is_verified         BOOLEAN DEFAULT FALSE,
    is_duplicate        BOOLEAN DEFAULT FALSE,
    metadata            JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_de_company ON distress_events(company_name);
CREATE INDEX IF NOT EXISTS idx_de_detected ON distress_events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_de_severity ON distress_events(severity);
CREATE INDEX IF NOT EXISTS idx_de_deal_score ON distress_events(deal_score DESC);
CREATE INDEX IF NOT EXISTS idx_de_source ON distress_events(source);
CREATE INDEX IF NOT EXISTS idx_de_category ON distress_events(signal_category);
CREATE INDEX IF NOT EXISTS idx_de_is_mmr ON distress_events(is_mmr);
CREATE INDEX IF NOT EXISTS idx_de_asset_class ON distress_events(asset_class);

-- ============================================================
-- DEMAND SIGNALS TABLE
-- Demand-side: tenant expansion, funding, BSE/NSE filings, hiring surges
-- ============================================================
CREATE TABLE IF NOT EXISTS demand_signals (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_name        TEXT NOT NULL,
    company_id          UUID REFERENCES companies(id) ON DELETE SET NULL,
    signal_type         TEXT NOT NULL CHECK (signal_type IN (
        'LEASE', 'OFFICE', 'RELOCATE', 'EXPAND', 'HIRING',
        'FUNDING', 'NEW_ENTRANT', 'DATACENTER', 'WAREHOUSE', 'LAND'
    )),
    confidence_score    NUMERIC(5, 2) CHECK (confidence_score BETWEEN 0 AND 100),
    urgency             TEXT DEFAULT 'MEDIUM' CHECK (urgency IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    space_type          TEXT,
    location            TEXT,
    sqft_mentioned      INTEGER,
    funding_amount_cr   NUMERIC(12, 2),
    why_cre             TEXT,                           -- human-readable reason
    suggested_action    TEXT,                           -- what broker should do
    summary             TEXT,
    source_url          TEXT,
    data_source         TEXT,                           -- BSE_FILING, RSS, NSE_FILING, LINKEDIN_JOBS etc.
    matched_phrases     TEXT[],
    published_at        TIMESTAMPTZ,
    detected_at         TIMESTAMPTZ DEFAULT NOW(),
    is_duplicate        BOOLEAN DEFAULT FALSE,
    metadata            JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    -- Full text search
    fts tsvector GENERATED ALWAYS AS (
        to_tsvector('english',
            coalesce(company_name,'') || ' ' ||
            coalesce(signal_type,'') || ' ' ||
            coalesce(location,'') || ' ' ||
            coalesce(summary,'') || ' ' ||
            coalesce(why_cre,'')
        )
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_ds_company ON demand_signals(company_name);
CREATE INDEX IF NOT EXISTS idx_ds_type ON demand_signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_ds_urgency ON demand_signals(urgency);
CREATE INDEX IF NOT EXISTS idx_ds_score ON demand_signals(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_ds_detected ON demand_signals(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_ds_fts ON demand_signals USING GIN(fts);

-- ============================================================
-- PRE-LEASED ASSETS TABLE
-- Grade A/B commercial properties with active leases — for investor pitches
-- ============================================================
CREATE TABLE IF NOT EXISTS pre_leased_assets (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name                        TEXT,
    address                     TEXT,
    micro_market                TEXT,
    city                        TEXT DEFAULT 'Mumbai',
    is_mmr                      BOOLEAN DEFAULT TRUE,
    asset_class                 TEXT CHECK (asset_class IN (
        'grade_a_office', 'grade_b_office', 'it_park', 'retail_mall',
        'retail_highstreet', 'industrial', 'hospitality', 'mixed_use'
    )),
    total_area_sqft             NUMERIC(14, 2),
    leased_area_sqft            NUMERIC(14, 2),
    occupancy_pct               NUMERIC(5, 2),
    tenant_name                 TEXT,
    tenant_category             TEXT CHECK (tenant_category IN (
        'blue_chip', 'institutional', 'government', 'listed_company',
        'mnc', 'startup', 'unknown'
    )),
    tenant_score                INTEGER DEFAULT 0 CHECK (tenant_score BETWEEN 0 AND 100),
    lease_start_date            DATE,
    lease_expiry_date           DATE,
    lock_in_months              INTEGER,
    rent_per_sqft               NUMERIC(10, 2),
    rent_escalation_pct         NUMERIC(5, 2) DEFAULT 15,
    escalation_frequency_years  INTEGER DEFAULT 3,
    asking_price_crore          NUMERIC(12, 2),
    gross_rent_annual_cr        NUMERIC(10, 3),
    noi_annual_cr               NUMERIC(10, 3),
    cap_rate_pct                NUMERIC(5, 2),
    yield_on_cost_10yr_pct      NUMERIC(6, 2),
    irr_estimate_pct            NUMERIC(5, 2),
    meets_investor_threshold    BOOLEAN DEFAULT FALSE,
    seller_type                 TEXT CHECK (seller_type IN (
        'bank_npa', 'arc', 'narcl', 'pe_exit', 'developer',
        'promoter_distress', 'family_office', 'other'
    )),
    urgency_level               TEXT DEFAULT 'normal' CHECK (
        urgency_level IN ('normal', 'motivated', 'distressed', 'desperate')
    ),
    strata_complications        BOOLEAN DEFAULT FALSE,
    oc_received                 BOOLEAN DEFAULT TRUE,
    source_event_id             UUID REFERENCES distress_events(id) ON DELETE SET NULL,
    source_url                  TEXT,
    notes                       TEXT,
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pla_micro_market ON pre_leased_assets(micro_market);
CREATE INDEX IF NOT EXISTS idx_pla_cap_rate ON pre_leased_assets(cap_rate_pct DESC);
CREATE INDEX IF NOT EXISTS idx_pla_threshold ON pre_leased_assets(meets_investor_threshold);
CREATE INDEX IF NOT EXISTS idx_pla_urgency ON pre_leased_assets(urgency_level);

-- ============================================================
-- CAP RATE SNAPSHOTS TABLE
-- Weekly market intelligence: rent, yield, cap rates per micro-market
-- ============================================================
CREATE TABLE IF NOT EXISTS cap_rate_snapshots (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    micro_market    TEXT NOT NULL,
    asset_class     TEXT NOT NULL,
    cap_rate_pct    NUMERIC(5, 2),
    avg_rent_psf    NUMERIC(10, 2),
    avg_price_psf   NUMERIC(12, 2),
    sample_size     INTEGER DEFAULT 1,
    snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
    source          TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(micro_market, asset_class, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_crs_market ON cap_rate_snapshots(micro_market);
CREATE INDEX IF NOT EXISTS idx_crs_date ON cap_rate_snapshots(snapshot_date DESC);

-- ============================================================
-- DEAL MATCHES TABLE
-- Cross-signal: distressed supply ↔ tenant demand matched
-- ============================================================
CREATE TABLE IF NOT EXISTS deal_matches (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    supply_event_id     UUID REFERENCES distress_events(id) ON DELETE CASCADE,
    demand_signal_id    UUID REFERENCES demand_signals(id) ON DELETE CASCADE,
    match_score         INTEGER DEFAULT 0 CHECK (match_score BETWEEN 0 AND 100),
    match_reason        TEXT,
    location_overlap    BOOLEAN DEFAULT FALSE,
    size_compatible     BOOLEAN DEFAULT FALSE,
    timing_overlap      BOOLEAN DEFAULT FALSE,
    broker_action       TEXT,
    status              TEXT DEFAULT 'new' CHECK (
        status IN ('new', 'reviewed', 'pursuing', 'closed', 'dead')
    ),
    assigned_to         TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(supply_event_id, demand_signal_id)
);

CREATE INDEX IF NOT EXISTS idx_dm_score ON deal_matches(match_score DESC);
CREATE INDEX IF NOT EXISTS idx_dm_status ON deal_matches(status);

-- ============================================================
-- LEAD SCORES TABLE
-- Per-company aggregate scores for both demand and distress
-- ============================================================
CREATE TABLE IF NOT EXISTS lead_scores (
    company_id          UUID REFERENCES companies(id) ON DELETE CASCADE PRIMARY KEY,
    demand_score        INTEGER DEFAULT 0,
    distress_score      INTEGER DEFAULT 0,
    combined_score      INTEGER DEFAULT 0,
    signal_count        INTEGER DEFAULT 0,
    priority_level      TEXT DEFAULT 'LOW' CHECK (priority_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CRAWLER RUNS TABLE
-- Audit log of every crawl execution
-- ============================================================
CREATE TABLE IF NOT EXISTS crawler_runs (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id              TEXT NOT NULL,
    source_name         TEXT,
    crawler_group       TEXT,
    status              TEXT CHECK (status IN ('started', 'completed', 'failed', 'partial')),
    events_found        INTEGER DEFAULT 0,
    events_inserted     INTEGER DEFAULT 0,
    error_message       TEXT,
    duration_seconds    NUMERIC(10, 2),
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ
);

-- ============================================================
-- INVESTOR MANDATES TABLE
-- Buyer-side: what PE funds, family offices, foreign investors want
-- ============================================================
CREATE TABLE IF NOT EXISTS investor_mandates (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_name       TEXT NOT NULL,
    investor_type       TEXT CHECK (investor_type IN (
        'pe_fund', 'family_office', 'reit', 'foreign_institutional',
        'hni', 'arc', 'bank', 'nbfc', 'other'
    )),
    min_deal_size_cr    NUMERIC(12, 2),
    max_deal_size_cr    NUMERIC(12, 2),
    min_cap_rate_pct    NUMERIC(5, 2),
    preferred_cities    TEXT[],
    preferred_asset_class TEXT[],
    preferred_tenants   TEXT[],
    notes               TEXT,
    source_url          TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    last_verified_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================
ALTER TABLE distress_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE demand_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE pre_leased_assets ENABLE ROW LEVEL SECURITY;
ALTER TABLE cap_rate_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE deal_matches ENABLE ROW LEVEL SECURITY;
ALTER TABLE lead_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE crawler_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE investor_mandates ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read distress_events" ON distress_events FOR SELECT USING (true);
CREATE POLICY "Public read demand_signals" ON demand_signals FOR SELECT USING (true);
CREATE POLICY "Public read companies" ON companies FOR SELECT USING (true);
CREATE POLICY "Public read pre_leased_assets" ON pre_leased_assets FOR SELECT USING (true);
CREATE POLICY "Public read cap_rate_snapshots" ON cap_rate_snapshots FOR SELECT USING (true);
CREATE POLICY "Public read deal_matches" ON deal_matches FOR SELECT USING (true);
CREATE POLICY "Public read lead_scores" ON lead_scores FOR SELECT USING (true);
CREATE POLICY "Public read crawler_runs" ON crawler_runs FOR SELECT USING (true);
CREATE POLICY "Public read investor_mandates" ON investor_mandates FOR SELECT USING (true);

CREATE POLICY "Anon insert distress_events" ON distress_events FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon insert demand_signals" ON demand_signals FOR INSERT WITH CHECK (true);
CREATE POLICY "Anon upsert companies" ON companies FOR ALL USING (true);
CREATE POLICY "Anon upsert pre_leased_assets" ON pre_leased_assets FOR ALL USING (true);
CREATE POLICY "Anon upsert cap_rate_snapshots" ON cap_rate_snapshots FOR ALL USING (true);
CREATE POLICY "Anon upsert deal_matches" ON deal_matches FOR ALL USING (true);
CREATE POLICY "Anon upsert lead_scores" ON lead_scores FOR ALL USING (true);
CREATE POLICY "Anon insert crawler_runs" ON crawler_runs FOR ALL USING (true);
CREATE POLICY "Anon upsert investor_mandates" ON investor_mandates FOR ALL USING (true);

-- ============================================================
-- VIEWS
-- ============================================================

CREATE OR REPLACE VIEW v_hot_deals AS
SELECT
    de.id,
    de.company_name,
    de.signal_category,
    de.asset_class,
    de.location,
    de.price_crore,
    de.deal_score,
    de.severity,
    de.is_mmr,
    de.channel,
    de.headline,
    de.url,
    de.detected_at,
    de.published_at,
    c.risk_score,
    c.distress_status
FROM distress_events de
LEFT JOIN companies c ON c.id = de.company_id
WHERE de.deal_score >= 60
  AND de.is_duplicate = FALSE
  AND de.detected_at > NOW() - INTERVAL '30 days'
ORDER BY de.deal_score DESC, de.detected_at DESC;

CREATE OR REPLACE VIEW v_hot_demand AS
SELECT
    ds.id,
    ds.company_name,
    ds.signal_type,
    ds.urgency,
    ds.confidence_score,
    ds.location,
    ds.sqft_mentioned,
    ds.funding_amount_cr,
    ds.why_cre,
    ds.suggested_action,
    ds.summary,
    ds.detected_at,
    c.demand_score,
    c.sector
FROM demand_signals ds
LEFT JOIN companies c ON c.id = ds.company_id
WHERE ds.confidence_score >= 55
  AND ds.is_duplicate = FALSE
  AND ds.detected_at > NOW() - INTERVAL '14 days'
ORDER BY ds.confidence_score DESC, ds.detected_at DESC;

CREATE OR REPLACE VIEW v_deal_matches_full AS
SELECT
    dm.id AS match_id,
    dm.match_score,
    dm.match_reason,
    dm.status,
    dm.broker_action,
    dm.created_at AS matched_at,
    -- supply side
    de.company_name AS supply_company,
    de.asset_class,
    de.location AS supply_location,
    de.price_crore,
    de.deal_score,
    de.channel,
    de.headline AS supply_headline,
    -- demand side
    ds.company_name AS demand_company,
    ds.signal_type,
    ds.urgency,
    ds.confidence_score,
    ds.location AS demand_location,
    ds.sqft_mentioned,
    ds.why_cre,
    ds.suggested_action
FROM deal_matches dm
JOIN distress_events de ON de.id = dm.supply_event_id
JOIN demand_signals ds ON ds.id = dm.demand_signal_id
ORDER BY dm.match_score DESC;

CREATE OR REPLACE VIEW v_upcoming_auctions AS
SELECT
    de.id,
    de.company_name,
    de.asset_class,
    de.location,
    de.price_crore,
    de.deal_score,
    de.headline,
    de.url,
    de.order_date,
    de.detected_at,
    de.is_mmr
FROM distress_events de
WHERE de.signal_category = 'auction'
  AND de.is_duplicate = FALSE
  AND (de.order_date IS NULL OR de.order_date >= CURRENT_DATE - INTERVAL '7 days')
ORDER BY de.order_date ASC NULLS LAST, de.deal_score DESC;

CREATE OR REPLACE VIEW v_cap_rate_trend AS
SELECT
    micro_market,
    asset_class,
    snapshot_date,
    cap_rate_pct,
    avg_rent_psf,
    avg_price_psf,
    source,
    LAG(cap_rate_pct) OVER (PARTITION BY micro_market, asset_class ORDER BY snapshot_date) AS prev_cap_rate,
    cap_rate_pct - LAG(cap_rate_pct) OVER (PARTITION BY micro_market, asset_class ORDER BY snapshot_date) AS cap_rate_change
FROM cap_rate_snapshots
ORDER BY micro_market, snapshot_date DESC;

-- ============================================================
-- SEED DATA — Investor Mandates
-- ============================================================
INSERT INTO investor_mandates (investor_name, investor_type, min_deal_size_cr, max_deal_size_cr, min_cap_rate_pct, preferred_cities, preferred_asset_class, notes, is_active) VALUES
    ('London-based Investor Group', 'foreign_institutional', 50, 500, 8.5, ARRAY['Mumbai', 'Pune'], ARRAY['grade_a_office'], 'Pre-leased grade A, 3 escalations @ 15% over 10 years', true),
    ('Blackstone India RE', 'pe_fund', 500, 5000, 7.0, ARRAY['Mumbai', 'Bengaluru', 'Hyderabad', 'Pune'], ARRAY['grade_a_office', 'it_park'], 'Large portfolios only, REIT-eligible assets', true),
    ('Brookfield India', 'pe_fund', 300, 3000, 7.5, ARRAY['Mumbai', 'Bengaluru', 'Delhi', 'Hyderabad'], ARRAY['grade_a_office', 'it_park'], 'Long hold, REIT strategy', true),
    ('GIC Singapore', 'foreign_institutional', 200, 2000, 7.5, ARRAY['Mumbai', 'Bengaluru'], ARRAY['grade_a_office'], 'Core-plus strategy, blue-chip tenants', true),
    ('Kotak Realty Fund', 'pe_fund', 50, 300, 8.0, ARRAY['Mumbai', 'Pune', 'Bengaluru'], ARRAY['grade_a_office', 'grade_b_office'], 'Domestic fund, MMR focus', true),
    ('HDFC Capital', 'nbfc', 30, 200, 8.5, ARRAY['Mumbai', 'Pune', 'Hyderabad', 'Bengaluru'], ARRAY['grade_a_office', 'it_park'], 'Pre-leased, yield-focused', true)
ON CONFLICT DO NOTHING;
