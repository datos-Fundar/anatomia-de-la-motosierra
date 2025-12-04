import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List
from sqlalchemy import exists
from chainsaw.model.tree import Tree
from chainsaw.pipeline.step import PipelineStep
from chainsaw.model.official_document import (
    Objective,
    Prompt,
)


class Pipeline:
    @classmethod
    def start(
        cls,
        session,
        tree: Tree,
        steps: List[PipelineStep],
        uuids: Optional[List[str]] = None,
        override: bool = False,
    ):
        if uuids is None:
            uuids = [unit.uuid for unit in tree.units]
        if uuids is not None and not override:
            uuids = [
                uuid
                for uuid in uuids
                if not session.query(
                    exists().where(
                        (Prompt.unit_uuid == uuid) &
                        (Prompt.tree_id == tree.id) &
                        (Objective.prompt_id == Prompt.id)
                    )
                ).scalar()
            ]

        db_url = str(session.get_bind().url)
        try:
            for step in steps:
                step._execute(
                    db_url,
                    tree,
                    uuids,
                )
        except Exception as e:
            now = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires"))
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            with open(os.path.join("errors.txt"), "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {str(e)}\n")
            raise e
