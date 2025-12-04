from tqdm import tqdm
from sqlalchemy import and_
from sqlalchemy.orm import selectinload
from typing import List, override
from chainsaw.model.tree import Tree
from chainsaw.model.node import Unit
from chainsaw.model.official_document import (
    Prompt,
    OfficialDocument,
    ScrappedDocument,
    ScrappedBlock,
)
from chainsaw.pipeline.step import PipelineStep
from chainsaw.pipeline.constants import (
    UNIT_NOT_FOUND,
    DOCUMENT_DATE,
    DOCUMENT_URL,
    DOCUMENT_CONTENT,
)


def __LLM_OBJECTIVES_PROMPT__(unit_name: str, full_text: str) -> str:
    return f"""
    Dado el siguiente texto, extraé las responsabilidades, acciones, objetivos y funciones relacionadas con la unidad estatal {unit_name}.
    * Se proveen múltiples bloques de texto, separados por '==='. Cada bloque tiene una fecha indicada por el texto {DOCUMENT_DATE}. Luego, el texto {DOCUMENT_CONTENT} indica el comienzo del bloque de texto.
    * El bloque generalmente tendrá encabezados con jerarquías de unidades. Las competencias, responsabilidades y acciones hacen referencia a la última unidad del encabezado, por lo que buscamos bloques que contengan el nombre de la unidad deseada e inmediatamente la palabra "responsabilidad primaria", "acciones" u "objetivos". En el caso de competencias, la palabra "compete" aparecerá antes del nombre de la unidad deseada.
    * Si entre el nombre de la unidad deseada y la primer ocurrencia de "acciones", "objetivos" o "responsabilidad primaria" figuran otras unidades, se debe ignorar el bloque.
    * No agregues introducciones, explicaciones ni encabezados del estilo "Aquí te presento la lista...".
    * Solo devolvé la lista de responsabilidades mas recientes (descartá responsabilidades previas), manteniendo titulares tales como "Responsabilidad primaria" o "Acciones" si están presentes y conservando en lo posible el texto original.
    * El texto resultante siempre debe estar en español.
    * Si por algún motivo no encontrás a la unidad y sus responsabilidades en el texto provisto, devolvé unicamente el mensaje "{UNIT_NOT_FOUND}".
    * Tene en cuenta que pueden ocurrir los siguientes casos:
        * La unidad mencionada no es la última del encabezado, por lo tanto las responsabilidades primarias son de otra unidad, dependiente de la unidad mencionada.
        * Puede pasar que la última unidad del encabezado contenga el nombre de la unidad mencionada pero no sea exactamente igual ("Dirección Nacional de Fiscalización del Trabajo" contiene a "Dirección Nacional de Fiscalización").
        * Se puede proveer un texto que no tenga encabezado pero que contenga explícitamente a la unidad mencionada. Probablemente sea una responsabilidad o acción de una segunda unidad que tiene que realizar algo en consecuencia de una acción de la unidad mencionada.
        Ignorá las responsabilidades y acciones de las unidades que correspondan a estos casos. 
A continuación, el texto completo: {full_text}"""


class Prompting(PipelineStep):
    @classmethod
    def __dated_content(
        cls,
        block: ScrappedBlock,
    ) -> str:
        return f"""===
{DOCUMENT_DATE} {block.scrapped_document.date.strftime('%d/%m/%Y')}
{DOCUMENT_URL} {block.scrapped_document.url}
{DOCUMENT_CONTENT} {block.text}
==="""

    @classmethod
    def __create_prompt_for(
        cls,
        unit: Unit,
        session,
    ) -> None:
        blocks = session.query(ScrappedBlock)\
            .options(selectinload(ScrappedBlock.scrapped_document)
                     .selectinload(ScrappedDocument.official_document))\
            .join(
                ScrappedDocument,
                (ScrappedDocument.id == ScrappedBlock.scrapped_document_id))\
            .join(
                OfficialDocument,
                (OfficialDocument.id == ScrappedDocument.official_document_id))\
            .filter(
                OfficialDocument.tree_id == unit.tree_id,
                OfficialDocument.related_unit_uuids.contains(unit.uuid),
                ScrappedBlock.unit_uuid == unit.uuid,
            )\
            .all()

        filtered_paragraphs = []
        for block in blocks:
            filtered_paragraphs.append((
                block.scrapped_document,
                cls.__dated_content(block)
            ))

        if filtered_paragraphs:
            urls = "; ".join([document.url for document, content in filtered_paragraphs])
            text = "\n".join([content for document, content in filtered_paragraphs])
            prompt_text = __LLM_OBJECTIVES_PROMPT__(unit.name, text)
            prompt = Prompt(
                text=prompt_text,
                urls=urls,
                unit_uuid=unit.uuid,
                tree_id=unit.tree_id,
            )
            session.add(prompt)
            session.commit()

    @override
    def _execute(
        self,
        db_url: str,
        tree: Tree,
        uuids: List[str],
    ):
        session = self._session_on(db_url)
        units = session.query(Unit)\
            .outerjoin(
                Prompt,
                and_(
                    Prompt.unit_uuid == Unit.uuid,
                    Prompt.tree_id == Unit.tree_id,
                )
            )\
            .filter(
                Unit.tree_id == tree.id,
                Unit.uuid.in_(uuids),
                Prompt.id.is_(None)
            )\
            .all()

        for unit in tqdm(units, total=len(units), desc="Generando prompts"):
            self.__create_prompt_for(unit, session)
        session.close()
