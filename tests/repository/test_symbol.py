from repository import RepositorySymbol, SymbolGroup


def test_fetch_symbols() -> None:
    rp = RepositorySymbol()
    # test arg default
    expd_symbols = ['AAPL', 'AMGN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS', 'DOW', 'GS', 'HD', 'HON', 'IBM',
                    'INTC', 'JNJ', 'JPM', 'KO', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH', 'V', 'VZ',
                    'WBA', 'WMT', 'TQQQ', 'SQQQ', 'SPY', 'QQQ', 'UVXY', 'XLF', 'SOXL', 'LABU', 'EEM', 'HYG', 'SH',
                    'FXI', 'XLE', 'ARKK', 'PSQ', 'IWM', 'EFA', 'EWZ', 'SPXU', 'SLV', 'SPXS', 'KWEB', 'GDX', 'VEA',
                    'TLT', 'LQD', 'XLP', 'IEMG', 'XLU', 'VWO']
    symbols_default = rp.fetch_symbols()
    assert expd_symbols == symbols_default
    # test arg group all
    symbols_all = rp.fetch_symbols(SymbolGroup.ALL)
    assert expd_symbols == symbols_all

