from unittest.mock import MagicMock, patch

import pytest

from src.extractors.google_ads import GoogleAdsExtractor


@pytest.fixture
def credentials():
    return {
        "developer_token": "test_token",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "refresh_token": "test_refresh_token",
        "login_customer_id": "1234567890",
    }


@pytest.fixture
def extractor(credentials):
    return GoogleAdsExtractor(credentials)


class TestListAccounts:
    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_returns_enabled_non_manager_accounts(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client

        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service

        # Simular resposta da API com conta ativa e conta manager
        enabled_row = MagicMock()
        enabled_row.customer_client.id = 111111
        enabled_row.customer_client.descriptive_name = "Cliente Ativo"
        enabled_row.customer_client.manager = False
        enabled_row.customer_client.status.name = "ENABLED"

        manager_row = MagicMock()
        manager_row.customer_client.id = 222222
        manager_row.customer_client.descriptive_name = "Manager Account"
        manager_row.customer_client.manager = True
        manager_row.customer_client.status.name = "ENABLED"

        disabled_row = MagicMock()
        disabled_row.customer_client.id = 333333
        disabled_row.customer_client.descriptive_name = "Disabled Account"
        disabled_row.customer_client.manager = False
        disabled_row.customer_client.status.name = "CANCELED"

        mock_service.search.return_value = [enabled_row, manager_row, disabled_row]

        accounts = extractor.list_accounts()

        assert len(accounts) == 1
        assert accounts[0]["customer_id"] == "111111"
        assert accounts[0]["customer_name"] == "Cliente Ativo"


class TestExtract:
    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_returns_correct_schema(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client

        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service

        row = MagicMock()
        row.campaign.id = 1001
        row.campaign.name = "Campanha Google"
        row.customer.id = 111111
        row.customer.descriptive_name = "Cliente B"
        row.metrics.impressions = 1500
        row.metrics.clicks = 60
        row.metrics.cost_micros = 150000000
        row.metrics.conversions = 8.0
        row.segments.date = "2026-03-22"

        mock_service.search.return_value = [row]

        results = extractor.extract(["111111"], date="2026-03-22")

        assert len(results) == 1
        result = results[0]
        expected_keys = {
            "customer_id", "customer_name", "campaign_id", "campaign_name",
            "impressions", "clicks", "spend", "conversions", "date",
        }
        assert set(result.keys()) == expected_keys

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_converts_cost_micros_to_currency(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client

        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service

        row = MagicMock()
        row.campaign.id = 1001
        row.campaign.name = "Campanha"
        row.customer.id = 111111
        row.customer.descriptive_name = "Cliente"
        row.metrics.impressions = 1000
        row.metrics.clicks = 50
        row.metrics.cost_micros = 150000000  # 150.00 em moeda
        row.metrics.conversions = 5.0
        row.segments.date = "2026-03-22"

        mock_service.search.return_value = [row]

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results[0]["spend"] == 150.00

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_handles_api_error_per_account(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client

        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = Exception("API Error")

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results == []

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_handles_empty_response(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client

        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = []

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results == []
