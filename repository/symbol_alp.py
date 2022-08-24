from dataclasses import dataclass

from pandas import DataFrame


@dataclass
class RepositorySymbolAlp:
    tbl_name: str = 'alpaca_assets'

    def prepare(self) -> None:
        pass

    def store(self, df: DataFrame) -> None:
        pass

    def fetch(self) -> DataFrame:
        pass
