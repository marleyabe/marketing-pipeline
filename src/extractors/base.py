from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    @abstractmethod
    def list_accounts(self) -> list[dict]:
        """Return list of active accounts."""

    @abstractmethod
    def extract(self, account_ids: list[str], date: str) -> list[dict]:
        """Extract data for the given accounts and date."""
