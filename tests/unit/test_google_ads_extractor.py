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
    view_through_conversions=1.0,
    all_conversions=4.0,
    search_impression_share=0.85,
    quality_score=7,
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
    row.ad_group_criterion.quality_info.quality_score = quality_score
    row.metrics.impressions = impressions
    row.metrics.clicks = clicks
    row.metrics.cost_micros = cost_micros
    row.metrics.conversions = conversions
    row.metrics.view_through_conversions = view_through_conversions
    row.metrics.all_conversions = all_conversions
    row.metrics.search_impression_share = search_impression_share
    row.segments.date = date
    return row


def _make_account_row(customer_id=111111, name="Cliente Ativo", manager=False, status="ENABLED"):
    row = MagicMock()
    row.customer_client.id = customer_id
    row.customer_client.descriptive_name = name
    row.customer_client.manager = manager
    row.customer_client.status.name = status
    return row


class TestListAccounts:
    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_returns_enabled_non_manager_accounts(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [
            _make_account_row(111111, "Ativo"),
            _make_account_row(222222, "Manager", manager=True),
            _make_account_row(333333, "Cancelado", status="CANCELED"),
        ]

        accounts = extractor.list_accounts()

        assert len(accounts) == 1
        assert accounts[0]["customer_id"] == "111111"
        assert accounts[0]["customer_name"] == "Ativo"

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_returns_empty_when_no_active_accounts(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [
            _make_account_row(111111, "Manager", manager=True),
        ]

        assert extractor.list_accounts() == []

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_returns_multiple_active_accounts(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [
            _make_account_row(111111, "Cliente A"),
            _make_account_row(222222, "Cliente B"),
        ]

        accounts = extractor.list_accounts()

        assert len(accounts) == 2
        assert {a["customer_id"] for a in accounts} == {"111111", "222222"}


class TestExtractKeywords:
    @patch("src.extractors.google_ads.GoogleAdsClient")
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
            "view_through_conversions", "all_conversions",
            "search_impression_share", "quality_score",
            "date",
        }
        assert set(results[0].keys()) == expected_keys

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_converts_cost_micros_to_currency(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [_make_keyword_row(cost_micros=75000000)]

        results = extractor.extract(["111111"], date="2026-03-22")
        assert results[0]["spend"] == 75.0

    @patch("src.extractors.google_ads.GoogleAdsClient")
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

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_maps_new_metrics_correctly(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.return_value = [
            _make_keyword_row(
                view_through_conversions=2.0,
                all_conversions=5.0,
                search_impression_share=0.72,
                quality_score=9,
            )
        ]

        results = extractor.extract(["111111"], date="2026-03-22")
        result = results[0]
        assert result["view_through_conversions"] == 2.0
        assert result["all_conversions"] == 5.0
        assert result["search_impression_share"] == 0.72
        assert result["quality_score"] == 9

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

    @patch("src.extractors.google_ads.GoogleAdsClient")
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

    @patch("src.extractors.google_ads.GoogleAdsClient")
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
