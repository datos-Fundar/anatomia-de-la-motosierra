import re
from tqdm import tqdm
from typing import List, override
from chainsaw.model.tree import Tree
from chainsaw.pipeline.step import PipelineStep
from chainsaw.model.official_document import (
    OfficialDocument,
    ScrappedDocument,
)


class Cleaning(PipelineStep):
    @classmethod
    def __clean(cls, text_norm: str):
        text = re.sub(r'[\u200b\u200c\u200d\uFEFF]', '', text_norm)
        text = text.replace('\n', ' ')
        text = text.replace('\u00A0', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(
            r'\b(Sr|Sra|Srta|Dr|Dra|Lic|Ing|Av|Art|Dto|etc)\.', r'\1<dot>',
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r'(\d)\.(\d)', r'\1<dot>\2', text)

        parts = re.split(r'\.\s*', text)
        merged_paragraphs = [
            part.replace('<dot>', '.').strip()
            for part in parts
            if part.strip()
        ]
        return cls._normalize_text("\n".join(merged_paragraphs))

    @override
    def _execute(
        self,
        db_url: str,
        tree: Tree,
        _: List[str],
    ):
        session = self._session_on(db_url)
        documents = session.query(ScrappedDocument)\
            .join(
                OfficialDocument, ScrappedDocument.official_document_id == OfficialDocument.id
            )\
            .filter(OfficialDocument.tree_id == tree.id).all()

        for document in tqdm(documents, total=len(documents), desc="Normalizando documentos"):
            document.text = self.__clean(document.text)
            session.add(document)
            session.commit()
        session.close()
