import os
import time
import ollama
from tqdm import tqdm
from multiprocessing import Pool
from openai import OpenAI, APIStatusError
from typing import List, Optional, override
from chainsaw.model.tree import Tree
from chainsaw.enum.llm_models import LLMModel
from chainsaw.model.scrapping import LLMResult
from chainsaw.model.official_document import (
    Prompt,
    Objective,
)
from chainsaw.pipeline.step import PipelineStep
from chainsaw.pipeline.constants import UNIT_NOT_FOUND


SECONDS_TO_SLEEP = int(os.getenv("TIME_TO_SLEEP", 5))
LLM_MODEL_NAME = os.getenv("LLM_MODEL", "LLAMA3_INSTRUCT")
try:
    LLM_MODEL = getattr(LLMModel, LLM_MODEL_NAME)
except AttributeError:
    raise ValueError(f"Modelo LLM desconocido: {LLM_MODEL_NAME}")


client = OpenAI()


class PromptExecutor:
    @classmethod
    def execute(cls, prompt: Prompt) -> Optional[LLMResult]:
        try:
            if "gpt" in LLM_MODEL.value:
                return cls.__execute_openai(prompt)
            else:
                return cls.__execute_ollama(prompt)
        except APIStatusError as error:
            raise Exception(f"APIStatusError: {error.status_code} - {str(error)}")

    @classmethod
    def __execute_ollama(cls, prompt: Prompt) -> Optional[LLMResult]:
        llm_response = ollama.chat(
            model=LLM_MODEL.value,
            messages=[{"role": "user", "content": prompt.text}],
        )
        objectives = llm_response['choices'][0]['message']['content']
        return None if UNIT_NOT_FOUND in objectives else LLMResult(text=objectives, urls=prompt.urls)

    @classmethod
    def __execute_openai(cls, prompt: Prompt) -> Optional[LLMResult]:
        llm_response = client.chat.completions.create(
            model=LLM_MODEL.value,
            messages=[{"role": "user", "content": prompt.text}],
        )
        objectives = llm_response.choices[0].message.content
        return None if UNIT_NOT_FOUND in objectives else LLMResult(text=objectives, urls=prompt.urls)


class LLMExtraction(PipelineStep):
    processes_amount: int = 6

    @classmethod
    def _execute_prompt(
        cls,
        db_url: str,
        prompt: Prompt,
    ) -> None:
        try:
            session = cls._session_on(db_url)
            persisted_prompt = session.query(Prompt).filter(Prompt.id == prompt.id).one()
            time.sleep(SECONDS_TO_SLEEP)

            if (llm_result := PromptExecutor.execute(persisted_prompt)):
                if persisted_prompt.objective is not None:
                    persisted_prompt.objective.text = llm_result.text
                    persisted_prompt.objective.urls = llm_result.urls
                else:
                    objective = Objective(
                        text=llm_result.text,
                        urls=llm_result.urls,
                        prompt_id=persisted_prompt.id,
                    )
                    session.add(objective)
                session.commit()
            session.close()
        except Exception as e:
            raise Exception(f"{prompt.unit_uuid}: {str(e)}")

    @override
    def _execute(
        self,
        db_url: str,
        tree: Tree,
        uuids: List[str],
    ):
        session = self._session_on(db_url)
        prompts = session.query(Prompt)\
            .filter(
                Prompt.tree_id == tree.id,
                Prompt.unit_uuid.in_(uuids)
            )\
            .all()

        args = [(db_url, prompt) for prompt in prompts]
        with Pool(processes=self.processes_amount) as pool:
            for _ in tqdm(
                pool.starmap(self._execute_prompt, args),
                total=len(prompts),
                desc=f"Evaluando prompts para extraer objetivos mediante {LLM_MODEL_NAME}",
            ):
                pass
