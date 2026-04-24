-- Budget mensal por cliente/plataforma (em BRL por padrão).
-- Usado por gold.budget_pacing e endpoints /budget.

INSERT INTO ops.client_budget (account_id, platform, monthly_budget, currency, active) VALUES
    -- ('1234567890',    'google_ads', 10000.00, 'BRL', TRUE),
    -- ('act_987654321', 'meta_ads',    5000.00, 'BRL', TRUE)
    ('__PLACEHOLDER__', 'google_ads', 1.00, 'BRL', FALSE)
ON CONFLICT (account_id, platform) DO UPDATE SET
    monthly_budget = EXCLUDED.monthly_budget,
    currency       = EXCLUDED.currency,
    active         = EXCLUDED.active,
    updated_at     = CURRENT_TIMESTAMP;

DELETE FROM ops.client_budget WHERE account_id = '__PLACEHOLDER__';
