from dataclasses import dataclass
from typing import Tuple

@dataclass(frozen=True)
class Transaction:
    blockNumber: str
    to: str
    hash: str

@dataclass(frozen=True)
class Block:
    number: str
    transactions: Tuple[Transaction]