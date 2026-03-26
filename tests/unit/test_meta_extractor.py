from unittest.mock import MagicMock, patch

import pytest

from src.extractors.meta_ads import MetaAdsExtractor


@pytest.fixture
def credentials():
    return {
        "app_id": "test_app_id",
        "app_secret": "test_app_secret",
        "access_token": "test_access_token",
    }


@pytest.fixture
def extractor(credentials):
    return MetaAdsExtractor(credentials)


class TestListAccounts:
    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.User")
    def test_returns_only_active_accounts(self, mock_user_cls, mock_api_cls, extractor):
        mock_api = MagicMock()
        mock_api_cls.init.return_value = mock_api

        active_account = MagicMock()
        active_account.__getitem__ = lambda self, key: {
            "account_id": "act_123",
            "name": "Active Account",
            "account_status": 1,
        }[key]

        inactive_account = MagicMock()
        inactive_account.__getitem__ = lambda self, key: {
            "account_id": "act_456",
            "name": "Inactive Account",
            "account_status": 2,
        }[key]

        mock_user = MagicMock()
        mock_user.get_ad_accounts.return_value = [active_account, inactive_account]
        mock_user_cls.return_value = mock_user

        accounts = extractor.list_accounts()

        assert len(accounts) == 1
        assert accounts[0]["account_id"] == "act_123"
        assert accounts[0]["account_name"] == "Active Account"

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.User")
    def test_excludes_inactive_accounts(self, mock_user_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        inactive_account = MagicMock()
        inactive_account.__getitem__ = lambda self, key: {
            "account_id": "act_456",
            "name": "Inactive",
            "account_status": 2,
        }[key]

        mock_user = MagicMock()
        mock_user.get_ad_accounts.return_value = [inactive_account]
        mock_user_cls.return_value = mock_user

        accounts = extractor.list_accounts()
        assert len(accounts) == 0


class TestExtract:
    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_returns_correct_schema(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_insight = MagicMock()
        mock_insight.export_all_data.return_value = {
            "account_id": "act_123",
            "account_name": "Cliente A",
            "campaign_id": "camp_1",
            "campaign_name": "Campanha 1",
            "ad_id": "ad_1",
            "ad_name": "Anuncio 1",
            "impressions": "1000",
            "clicks": "50",
            "spend": "100.50",
            "date_start": "2026-03-22",
            "date_stop": "2026-03-22",
            "actions": [{"action_type": "link_click", "value": "50"}],
        }

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [mock_insight]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract(["act_123"], date="2026-03-22")

        assert len(results) == 1
        row = results[0]
        expected_keys = {
            "account_id", "account_name", "campaign_id", "campaign_name",
            "ad_id", "ad_name", "impressions", "clicks", "spend",
            "date_start", "date_stop", "actions",
            "device_platform", "publisher_platform",
        }
        assert set(row.keys()) == expected_keys

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_handles_api_error_gracefully(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.side_effect = Exception("API Error")
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract(["act_123"], date="2026-03-22")
        assert results == []

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_handles_empty_response(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = []
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract(["act_123"], date="2026-03-22")
        assert results == []

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_numeric_type_coercion(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_insight = MagicMock()
        mock_insight.export_all_data.return_value = {
            "account_id": "act_123",
            "account_name": "Cliente A",
            "campaign_id": "camp_1",
            "campaign_name": "Campanha 1",
            "ad_id": "ad_1",
            "ad_name": "Anuncio 1",
            "impressions": "1000",
            "clicks": "50",
            "spend": "100.50",
            "date_start": "2026-03-22",
            "date_stop": "2026-03-22",
            "actions": [],
        }

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [mock_insight]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract(["act_123"], date="2026-03-22")
        row = results[0]

        assert isinstance(row["impressions"], int)
        assert isinstance(row["clicks"], int)
        assert isinstance(row["spend"], float)

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_maps_device_platform_correctly(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_insight = MagicMock()
        mock_insight.export_all_data.return_value = {
            "account_id": "act_123",
            "account_name": "Cliente A",
            "campaign_id": "camp_1",
            "campaign_name": "Campanha 1",
            "ad_id": "ad_1",
            "ad_name": "Anuncio 1",
            "impressions": "500",
            "clicks": "20",
            "spend": "50.00",
            "date_start": "2026-03-26",
            "date_stop": "2026-03-26",
            "actions": [],
            "device_platform": "mobile_web",
            "publisher_platform": "facebook",
        }

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [mock_insight]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract(["act_123"], date="2026-03-26")
        row = results[0]

        assert row["device_platform"] == "mobile_web"
        assert row["publisher_platform"] == "facebook"

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_passes_device_breakdowns_to_api(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = []
        mock_adaccount_cls.return_value = mock_account

        extractor.extract(["act_123"], date="2026-03-26")

        call_kwargs = mock_account.get_insights.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs.args[1]
        assert "device_platform" in params["breakdowns"]
        assert "publisher_platform" in params["breakdowns"]
