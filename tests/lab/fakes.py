class FakePrices:
    """closes: {ticker: {"YYYY-MM-DD": price}}"""

    def __init__(self, closes, currencies=None):
        self.data, self.currencies = closes, currencies or {}

    def closes(self, ticker, start, end):
        return sorted((d, p) for d, p in self.data.get(ticker, {}).items() if start <= d < end)

    def currency(self, ticker):
        return self.currencies.get(ticker)
