from __future__ import annotations
from pydantic import BaseModel
from typing import Dict


class Token(BaseModel):
symbol: str
address: str
decimals: int


class Config(BaseModel):
chain_id: int
tokens: Dict[str, Token]
