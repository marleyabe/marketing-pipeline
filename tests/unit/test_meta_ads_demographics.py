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


def _make_demographics_insight(age="18-24", gender="male", impressions="500",
                                clicks="20", spend="50.00",
                                account_id="act_123", ad_id="ad_1"):
    mock = MagicMock()
    mock.export_all_data.return_value = {
        "account_id": account_id,
        "account_name": "Cliente A",
        "campaign_id": "camp_1",
        "campaign_name": "Campanha 1",
        "ad_id": ad_id,
        "ad_name": "Anuncio 1",
        "impressions": impressions,
        "clicks": clicks,
        "spend": spend,
        "date_start": "2026-03-26",
        "date_stop": "2026-03-26",
        "actions": [],
        "age": age,
        "gender": gender,
    }
    return mock


class TestExtractDemographics:

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_returns_correct_schema(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [_make_demographics_insight()]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract_demographics(["act_123"], date="2026-03-26")

        assert len(results) == 1
        row = results[0]
        expected_keys = {
            "account_id", "account_name", "campaign_id", "campaign_name",
            "ad_id", "ad_name", "age", "gender",
            "impressions", "clicks", "spend",
            "date_start", "date_stop", "actions",
        }
        assert set(row.keys()) == expected_keys

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_age_value_mapped_correctly(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [_make_demographics_insight(age="25-34")]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract_demographics(["act_123"], date="2026-03-26")
        assert results[0]["age"] == "25-34"

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_gender_value_mapped_correctly(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [_make_demographics_insight(gender="female")]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract_demographics(["act_123"], date="2026-03-26")
        assert results[0]["gender"] == "female"

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_spend_is_float(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [_make_demographics_insight(spend="123.45")]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract_demographics(["act_123"], date="2026-03-26")
        assert isinstance(results[0]["spend"], float)
        assert results[0]["spend"] == 123.45

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_passes_age_gender_breakdowns_to_api(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = []
        mock_adaccount_cls.return_value = mock_account

        extractor.extract_demographics(["act_123"], date="2026-03-26")

        call_kwargs = mock_account.get_insights.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs.args[1]
        assert "age" in params["breakdowns"]
        assert "gender" in params["breakdowns"]

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_handles_error_gracefully(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.side_effect = Exception("API Error")
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract_demographics(["act_123"], date="2026-03-26")
        assert results == []

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_error_in_one_account_does_not_stop_others(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        failing_account = MagicMock()
        failing_account.get_insights.side_effect = Exception("API Error")

        success_account = MagicMock()
        success_account.get_insights.return_value = [
            _make_demographics_insight(account_id="act_456", ad_id="ad_2")
        ]

        mock_adaccount_cls.side_effect = [failing_account, success_account]

        results = extractor.extract_demographics(["111111", "222222"], date="2026-03-26")

        assert len(results) == 1
        assert results[0]["account_id"] == "act_456"

    @patch("src.extractors.meta_ads.FacebookAdsApi")
    @patch("src.extractors.meta_ads.AdAccount")
    def test_multiple_age_gender_combinations(self, mock_adaccount_cls, mock_api_cls, extractor):
        mock_api_cls.init.return_value = MagicMock()

        mock_account = MagicMock()
        mock_account.get_insights.return_value = [
            _make_demographics_insight(age="18-24", gender="male"),
            _make_demographics_insight(age="18-24", gender="female"),
            _make_demographics_insight(age="25-34", gender="male"),
        ]
        mock_adaccount_cls.return_value = mock_account

        results = extractor.extract_demographics(["act_123"], date="2026-03-26")
        assert len(results) == 3
