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


def _make_gender_row(
    customer_id=111111,
    customer_name="Cliente A",
    campaign_id=1001,
    campaign_name="Campanha",
    ad_group_id=2001,
    ad_group_name="Grupo",
    gender_name="MALE",
    impressions=200,
    clicks=10,
    cost_micros=20000000,
    conversions=1.0,
    date="2026-03-22",
):
    row = MagicMock()
    row.customer.id = customer_id
    row.customer.descriptive_name = customer_name
    row.campaign.id = campaign_id
    row.campaign.name = campaign_name
    row.ad_group.id = ad_group_id
    row.ad_group.name = ad_group_name
    row.ad_group_criterion.gender.type_.name = gender_name
    row.metrics.impressions = impressions
    row.metrics.clicks = clicks
    row.metrics.cost_micros = cost_micros
    row.metrics.conversions = conversions
    row.segments.date = date
    return row


def _make_age_row(
    customer_id=111111,
    customer_name="Cliente A",
    campaign_id=1001,
    campaign_name="Campanha",
    ad_group_id=2001,
    ad_group_name="Grupo",
    age_range_name="AGE_RANGE_25_34",
    impressions=300,
    clicks=15,
    cost_micros=30000000,
    conversions=2.0,
    date="2026-03-22",
):
    row = MagicMock()
    row.customer.id = customer_id
    row.customer.descriptive_name = customer_name
    row.campaign.id = campaign_id
    row.campaign.name = campaign_name
    row.ad_group.id = ad_group_id
    row.ad_group.name = ad_group_name
    row.ad_group_criterion.age_range.type_.name = age_range_name
    row.metrics.impressions = impressions
    row.metrics.clicks = clicks
    row.metrics.cost_micros = cost_micros
    row.metrics.conversions = conversions
    row.segments.date = date
    return row


def _make_income_row(
    customer_id=111111,
    customer_name="Cliente A",
    campaign_id=1001,
    campaign_name="Campanha",
    ad_group_id=2001,
    ad_group_name="Grupo",
    income_range_name="INCOME_RANGE_50_60",
    impressions=150,
    clicks=8,
    cost_micros=16000000,
    conversions=0.5,
    date="2026-03-22",
):
    row = MagicMock()
    row.customer.id = customer_id
    row.customer.descriptive_name = customer_name
    row.campaign.id = campaign_id
    row.campaign.name = campaign_name
    row.ad_group.id = ad_group_id
    row.ad_group.name = ad_group_name
    row.ad_group_criterion.income_range.type_.name = income_range_name
    row.metrics.impressions = impressions
    row.metrics.clicks = clicks
    row.metrics.cost_micros = cost_micros
    row.metrics.conversions = conversions
    row.segments.date = date
    return row


class TestExtractDemographics:
    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_returns_correct_schema(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [_make_gender_row()],
            [_make_age_row()],
            [_make_income_row()],
        ]

        results = extractor.extract_demographics(["111111"], date="2026-03-22")

        assert len(results) == 3
        expected_keys = {
            "customer_id", "customer_name",
            "campaign_id", "campaign_name",
            "ad_group_id", "ad_group_name",
            "dimension_type", "dimension_value",
            "impressions", "clicks", "spend", "conversions",
            "date",
        }
        assert set(results[0].keys()) == expected_keys

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_gender_dimension_type_and_value(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [_make_gender_row(gender_name="FEMALE")],
            [],
            [],
        ]

        results = extractor.extract_demographics(["111111"], date="2026-03-22")

        assert results[0]["dimension_type"] == "gender"
        assert results[0]["dimension_value"] == "FEMALE"

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_age_range_dimension_type_and_value(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [],
            [_make_age_row(age_range_name="AGE_RANGE_35_44")],
            [],
        ]

        results = extractor.extract_demographics(["111111"], date="2026-03-22")

        assert results[0]["dimension_type"] == "age_range"
        assert results[0]["dimension_value"] == "AGE_RANGE_35_44"

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_income_range_dimension_type_and_value(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [],
            [],
            [_make_income_row(income_range_name="INCOME_RANGE_70_80")],
        ]

        results = extractor.extract_demographics(["111111"], date="2026-03-22")

        assert results[0]["dimension_type"] == "income_range"
        assert results[0]["dimension_value"] == "INCOME_RANGE_70_80"

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_converts_cost_micros_to_currency(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [_make_gender_row(cost_micros=50000000)],
            [],
            [],
        ]

        results = extractor.extract_demographics(["111111"], date="2026-03-22")

        assert results[0]["spend"] == 50.0

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_combines_all_three_dimensions(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        mock_service.search.side_effect = [
            [_make_gender_row(), _make_gender_row(gender_name="FEMALE")],
            [_make_age_row()],
            [_make_income_row()],
        ]

        results = extractor.extract_demographics(["111111"], date="2026-03-22")

        assert len(results) == 4
        types = [r["dimension_type"] for r in results]
        assert types.count("gender") == 2
        assert types.count("age_range") == 1
        assert types.count("income_range") == 1

    @patch("src.extractors.google_ads.GoogleAdsClient")
    def test_error_in_one_account_does_not_stop_others(self, mock_client_cls, extractor):
        mock_client = MagicMock()
        mock_client_cls.load_from_dict.return_value = mock_client
        mock_service = MagicMock()
        mock_client.get_service.return_value = mock_service
        # first account fails all 3 queries, second succeeds
        mock_service.search.side_effect = [
            Exception("API Error"),
            Exception("API Error"),
            Exception("API Error"),
            [_make_gender_row(customer_id=222222)],
            [],
            [],
        ]

        results = extractor.extract_demographics(["111111", "222222"], date="2026-03-22")

        assert len(results) == 1
        assert results[0]["customer_id"] == "222222"
