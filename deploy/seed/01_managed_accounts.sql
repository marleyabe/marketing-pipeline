-- Contas gerenciadas (quais account_ids puxar nos DAGs de extração).
-- Preencha antes de unpause dos DAGs em prod.
--
-- platform: 'google_ads' usa customer_id sem hífens (ex: '1234567890')
-- platform: 'meta_ads'   usa ad_account_id com prefixo 'act_' (ex: 'act_987654321')

INSERT INTO ops.managed_accounts (account_id, platform, account_name, enabled) VALUES
    -- ('1234567890',      'google_ads', 'Cliente A',  1),
    -- ('act_987654321',   'meta_ads',   'Cliente A',  1)
    ('__PLACEHOLDER__', 'google_ads', '__remover__', 0)
ON CONFLICT (account_id, platform) DO NOTHING;

DELETE FROM ops.managed_accounts WHERE account_id = '__PLACEHOLDER__';
