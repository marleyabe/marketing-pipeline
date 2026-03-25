from unittest.mock import MagicMock, patch

import pytest

from src.extractors.google_ads_keywords import GoogleAdsKeywordsExtractor


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
    return GoogleAdsKeywordsExtractor(credentials)


def _make_keyword_row(
    customer_id=111111,
    customer_name="Cliente A",
    campaign_id=1001,
    campaign_name="Campanha",
    ad_group_id=2001,
    ad_group_name="Grupo",
    criterion_id=3001,
    keyword_text="comprar tênis",
    match_type_name="EXACT",
    impressions=500,
    clicks=30,
    cost_micros=60000000,
    conversions=3.0,
    date="2026-03-22",
):
    row = MagicMock()
    row.customer.id = customer_id
    row.customer.descriptive_name = customer_name
    row.campaign.id = campaign_id
    row.campaign.name = campaign_name
    row.ad_group.id = ad_group_id
    row.ad_group.name = ad_group_name
    row.ad_group_criterion.criterion_id = criterion_id
    row.ad_group_criterion.keyword.text = keyword_text
    row.ad_group_criterion.keyword.match_type.name = match_type_name
    row.metrics.impressions = impressions
    row.metrics.clicks = clicks
    row.metrics.cost_micros = cost_micros
    row.metrics.conversions = conversions
    row.segments.date = date
    return row


class TestExtractKeywords:
    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_returns_correct_schema(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [_make_keyword_row()]

        results = extractor.extract(["111111"], date="2026-03-22")

        assert len(results) == 1
        expected_keys = {
            "customer_id", "customer_name",
            "campaign_id", "campaign_name",
            "ad_group_id", "ad_group_name",
            "keyword_id", "keyword_text", "match_type",
            "impressions", "clicks", "spend", "conversions",
            "date",
        }
        assert set(results[0].keys()) == expected_keys

    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_converts_cost_micros_to_currency(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [_make_keyword_row(cost_micros=75000000)]

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results[0]["spend"] == 75.0

    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_maps_keyword_fields_correctly(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [
            _make_keyword_row(
                keyword_text="tênis masculino",
                match_type_name="BROAD",
                criterion_id=9999,
            )
        ]

        results = extractor.extract(["111111"], date="2026-03-22")
        result = results[0]
        assert result["keyword_text"] == "tênis masculino"
        assert result["match_type"] == "BROAD"
        assert result["keyword_id"] == "9999"

    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_handles_api_error_per_account(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = Exception("API Error")

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results == []

    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_handles_empty_response(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = []

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results == []

    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_extracts_multiple_accounts(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [_make_keyword_row(customer_id=111111)],
            [_make_keyword_row(customer_id=222222)],
        ]

        results = extractor.extract(["111111", "222222"], date="2026-03-22")
        assert len(results) == 2

    @patch("src.extractors.google_ads_keywords.GoogleAdsClient")
    def test_error_in_one_account_does_not_stop_others(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            Exception("API Error"),
            [_make_keyword_row(customer_id=222222)],
        ]

        results = extractor.extract(["111111", "222222"], date="2026-03-22")
        assert len(results) == 1
        assert results[0]["customer_id"] == "222222"
