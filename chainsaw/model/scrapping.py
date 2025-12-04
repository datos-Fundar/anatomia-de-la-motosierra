from datetime import date
from typing import NamedTuple


class ScrappedInfo(NamedTuple):
    url: str
    text: str
    date: date


class LLMResult(NamedTuple):
    text: str
    urls: str
