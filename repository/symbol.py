from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


@dataclass
class RepositorySymbol:
    pwd: str = Path(__file__).resolve().parent

    def fetch_symbols(self, group: str = 'ALL') -> List[str]:
        with open(f'{self.pwd}/symbols.yml') as f:
            symbols = yaml.safe_load(f)
        if group == 'ALL':
            return sum(list(symbols.values()), [])
        else:
            return symbols[group]


def main() -> None:
    repo_symbol = RepositorySymbol()
    symbols = repo_symbol.fetch_symbols()
    print(symbols)


if __name__ == '__main__':
    main()
