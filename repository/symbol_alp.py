from dataclasses import dataclass

from pandas import DataFrame


@dataclass
class RepositorySymbolAlp:
    def prepare(self) -> None:
        pass

    def store(self, df: DataFrame) -> None:
        pass

    def fetch(self) -> DataFrame:
        pass
