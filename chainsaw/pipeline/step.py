import re
import unicodedata
from pydantic import BaseModel
from abc import ABC, abstractmethod
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from chainsaw.model.tree import Tree


class PipelineStep(ABC, BaseModel):
    @abstractmethod
    def _execute(
        self,
        db_url: str,
        tree: Tree,
        uuids: List[str],
    ):
        pass

    @classmethod
    def _session_on(cls, db_url):
        engine = create_engine(db_url)
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal()

    @classmethod
    def _normalize_text(cls, text: str) -> str:
        text = text.lower()
        text = unicodedata.normalize('NFKD', text)
        text = ''.join(c for c in text if not unicodedata.combining(c))
        text = re.sub(r"[^\w\s-]", "", text)
        text = text.replace("-", " ")
        return text
