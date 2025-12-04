import re
from tqdm import tqdm
from typing import List, Dict, override
from sqlalchemy.orm import selectinload
from chainsaw.model.node import Unit
from chainsaw.model.tree import Tree
from chainsaw.pipeline.step import PipelineStep
from chainsaw.pipeline.constants import KEY_PHRASES
from chainsaw.model.official_document import (
    OfficialDocument,
    ScrappedBlock,
)


class Finding(PipelineStep):
    @classmethod
    def __tokens_match_exact_sequence(
        cls,
        small: List[str],
        big: List[str],
    ) -> bool:
        n = len(small)
        for i in range(len(big) - n + 1):
            if big[i:i+n] == small:
                rest = " ".join(big[i+n : min(i+n+3, len(big))])

                for phrase in KEY_PHRASES:
                    if phrase != "compete " and rest.startswith(phrase):
                        return True

                window_before = " ".join(big[max(0, i-5):i])
                if re.search(r"\bcompete\s+a(l| la| el)?\b", window_before):
                    return True
        return False

    @classmethod
    def __get_unit_paragraphs_mapping(
        cls,
        names_by_uuid: Dict[str, str],
        paragraphs: List[str],
    ) -> Dict[int, str]:
        matching_idxs = {}
        for idx, paragraph in enumerate(paragraphs):
            candidate_uuid = None
            candidate_tokens_length = 0

            for unit_uuid, unit_name in names_by_uuid.items():
                unit_tokens = cls._normalize_text(unit_name).split()
                if cls.__tokens_match_exact_sequence(
                    unit_tokens,
                    paragraph.split(),
                ):
                    if len(unit_tokens) > candidate_tokens_length:
                        candidate_uuid = unit_uuid
                        candidate_tokens_length = len(unit_tokens)

            if candidate_uuid:
                matching_idxs[idx] = candidate_uuid
        return matching_idxs

    @classmethod
    def __build_blocks(
        cls,
        session,
        scrapped_document_id: int,
        paragraphs: List[str],
        unit_paragraph_idxs: Dict[int, int],
        max_block_len: int = 80,
        min_block_len: int = 5,
    ) -> None:
        if not unit_paragraph_idxs:
            return

        sorted_idxs = sorted(unit_paragraph_idxs.keys())
        n_paragraphs = len(paragraphs)

        for i, start_idx in enumerate(sorted_idxs):
            start = start_idx

            if i + 1 < len(sorted_idxs):
                end = sorted_idxs[i + 1]
            else:
                end = min(start + max_block_len, n_paragraphs)

            if end - start < min_block_len:
                end = min(start + min_block_len, n_paragraphs)
            end = min(end, start + max_block_len, n_paragraphs)
            block_text = " ".join(paragraphs[start:end]).strip()

            scrapped_block = ScrappedBlock(
                text=block_text,
                unit_uuid=unit_paragraph_idxs[start_idx],
                scrapped_document_id=scrapped_document_id,
            )
            session.add(scrapped_block)
            session.commit()

    @override
    def _execute(
        self,
        db_url: str,
        tree: Tree,
        _: List[str],
    ):
        session = self._session_on(db_url)
        documents = session.query(OfficialDocument)\
            .options(selectinload(OfficialDocument.scrapped_documents))\
            .filter(OfficialDocument.tree_id == tree.id)\
            .all()

        for document in tqdm(documents, total=len(documents), desc="Descubriendo párrafos relevantes"):
            names_by_uuid = {unit_uuid: session.query(Unit.name)
                             .filter(
                                Unit.tree_id == tree.id,
                                Unit.uuid == unit_uuid)
                             .one()[0]
                             for unit_uuid in document.related_unit_uuids}

            for scrapped in document.scrapped_documents:
                paragraphs = scrapped.text.split("\n")
                unit_paragraph_idxs = self.__get_unit_paragraphs_mapping(
                    names_by_uuid,
                    paragraphs,
                )
                self.__build_blocks(
                    session,
                    scrapped.id,
                    paragraphs,
                    unit_paragraph_idxs,
                )
        session.close()
