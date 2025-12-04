import os
import numpy as np
from tqdm import tqdm
from typing import List, override
import undetected_chromedriver as uc

from chainsaw.model.tree import Tree
from chainsaw.pipeline.step import PipelineStep
from chainsaw.pipeline.constants import KEY_PHRASES
from chainsaw.pipeline.scrapping.scrappers import OfficialDocumentScrapper
from chainsaw.model.official_document import (
    OfficialDocument,
    ScrappedDocument,
)


class Scrapping(PipelineStep):
    @classmethod
    def __get_driver(cls):
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument('--log-level=3')
        options.add_argument("--disable-dev-shm-usage")
        return uc.Chrome(
            version_main=int(os.getenv("CHROME_MAIN_VERSION", 137)),
            options=options,
        )

    @classmethod
    def __trimmed_mean(
        cls,
        paragraphs,
        lower_percentile=5,
        upper_percentile=95,
    ):
        lengths = [len(paragraph) for paragraph in paragraphs]
        data = np.array(lengths)
        lower = np.percentile(data, lower_percentile)
        upper = np.percentile(data, upper_percentile)
        trimmed = data[(data >= lower) & (data <= upper)]
        return trimmed.mean() if trimmed.size > 0 else 0

    @classmethod
    def __has_responsabilities(cls, text: str) -> bool:
        if any((phrase in text for phrase in KEY_PHRASES)):
            paragraphs = [paragraph
                          for raw_paragraph in text.split('\n')
                          if (paragraph := raw_paragraph.strip())
                          and len(paragraph) >= 3]
            average_length = cls.__trimmed_mean(paragraphs)
            return int(average_length) > 30
        else:
            return False

    @classmethod
    def __scrapping_document(
        cls,
        document: OfficialDocument,
        session,
        driver,
    ) -> None:
        if len(document.scrapped_documents) == 0 and not document.processed:
            scrapped = OfficialDocumentScrapper.working_on(
                document.url,
                driver,
            )
            for scrapped_info in scrapped:
                if cls.__has_responsabilities(scrapped_info.text):
                    scrapped = ScrappedDocument(
                        official_document_id=document.id,
                        url=scrapped_info.url,
                        text=scrapped_info.text,
                        date=scrapped_info.date,
                    )
                    session.add(scrapped)
            document.processed = True
            session.commit()

    @override
    def _execute(
        self,
        db_url: str,
        tree: Tree,
        _: List[str],
    ):
        driver = self.__get_driver()
        session = self._session_on(db_url)

        documents = session.query(OfficialDocument).filter(
            OfficialDocument.tree_id == tree.id,
            OfficialDocument.processed.is_(False)).all()
        for document in tqdm(documents, total=len(documents), desc="Scrappeando documentos"):
            self.__scrapping_document(
                document,
                session,
                driver,
            )
        driver.quit()
        session.close()
