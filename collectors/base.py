class BaseCollector:
    def __init__(self, mock: bool = False):
        self.mock = mock

    def fetch(self, start_date: str, end_date: str) -> dict:
        raise NotImplementedError

    def _mock_data(self) -> dict:
        raise NotImplementedError
