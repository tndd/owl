from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List

import yaml


class SymbolGroup(Enum):
    ALL = 'all'


@dataclass
class RepositorySymbol:
    pwd: str = Path(__file__).resolve().parent

    def fetch_symbols(self, group: SymbolGroup = SymbolGroup.ALL) -> List[str]:
        with open(f'{self.pwd}/general/symbols.yml') as f:
            symbols = yaml.safe_load(f)
        if group == SymbolGroup.ALL:
            return sum(list(symbols.values()), [])
        else:
            return symbols[group]


def main() -> None:
    repo_symbol = RepositorySymbol()
    symbols = repo_symbol.fetch_symbols()
    print(symbols)


if __name__ == '__main__':
    main()
