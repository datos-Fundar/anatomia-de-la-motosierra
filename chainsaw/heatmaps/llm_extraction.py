import os
import re
import json
import numpy as np
from tqdm import tqdm
from typing import List, Dict, Any, Literal, Tuple
from openai import OpenAI
from collections import defaultdict
from chainsaw.enum.llm_models import LLMModel
from chainsaw.heatmaps.utils import units_on_cluster
from sklearn.metrics.pairwise import cosine_similarity
from chainsaw.heatmaps.constants import (
    EXPECTED_ENVIRONMENTS,
    DimensionName,
    LLM_BASED_DIMENSIONS,
)


LLM_MODEL_NAME = os.getenv("LLM_MODEL", "LLAMA3_INSTRUCT")
try:
    LLM_MODEL = getattr(LLMModel, LLM_MODEL_NAME)
except AttributeError:
    raise ValueError(f"Modelo LLM desconocido: {LLM_MODEL_NAME}")


class LLMExtraction:
    def __init__(self, clusters_file_name: str) -> None:
        with open(f"clusters/{clusters_file_name}.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        self.clusters_json = data
        self.client = OpenAI()
        self.clusters_file_name = clusters_file_name
        self.occurrences_results = {}
        self.results_to_reuse = {
            DimensionName.OBJECTIVE.value: defaultdict(list),
            DimensionName.TARGET.value: defaultdict(list),
            DimensionName.ENVIRONMENT.value: defaultdict(list),
        }

    # General
    def __open_prompt(self, prompt_file_name: str) -> str:
        with open(f"../chainsaw/heatmaps/prompts/{prompt_file_name}.txt", "r", encoding="utf-8") as f:
            prompt = f.read()
        return prompt

    def __send_prompt(self, prompt, params) -> Dict[str, Any]:
        text = f"{prompt} {params}"
        llm_response = self.client.chat.completions.create(
            model=LLM_MODEL.value,
            messages=[{"role": "user", "content": text}],
        )
        raw = llm_response.choices[0].message.content
        cleaned = re.sub(r"^```json\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        cleaned = cleaned.replace("\\n", "\n")
        return json.loads(cleaned)

    def __create_embeddings(self, occurrences: List[Dict[str, Any]]):
        response = self.client.embeddings.create(
            model="text-embedding-3-small",
            input=[occurrence["tag"] for occurrence in occurrences]
        )
        return [d.embedding for d in response.data]

    def __compute_similarities(self, occurrences1, occurrences2):
        embeddings1 = np.array(self.__create_embeddings(occurrences1))
        embeddings2 = np.array(self.__create_embeddings(occurrences2))
        return cosine_similarity(embeddings1, embeddings2)

    def __look_for_environments_in_unit(
        self,
        environments,
        text,
        idx=0,
        is_name=False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        words = re.findall(r"\w+", text.lower())
        occurrences = []
        for environment in environments:
            environment_tokens = re.findall(r"\w+", environment)
            n = len(environment_tokens)
            for i in range(len(words) - n + 1):
                if words[i:i+n] == environment_tokens:
                    occurrence = {
                        "id": idx,
                        "tag": environment,
                        "desde": i,
                        "hasta": i + n
                    }
                    if is_name:
                        occurrence["nombre"] = True
                    occurrences.append(occurrence)
                    idx = idx + 1
        return occurrences, idx

    def __preprocess_environment_dimension_by_unit(self, cluster_units) -> None:
        results = {}
        dimension_name = DimensionName.ENVIRONMENT.value
        environments = [environment.lower() for environment in EXPECTED_ENVIRONMENTS]
        
        for unit in tqdm(cluster_units, total=len(cluster_units), desc=dimension_name, leave=False):
            idx = 0
            all_occurrences = []

            try:
                results[unit["uuid"]] = self.results_to_reuse[dimension_name][unit["uuid"]][0]['unidad_1']
            except (KeyError, IndexError):
                for text, is_name in [(unit["objective"], False), (unit["name"], True)]:
                    occurrences, idx = self.__look_for_environments_in_unit(
                        environments=environments,
                        text=text,
                        idx=idx,
                        is_name=is_name,
                    )
                    if occurrences:
                        all_occurrences.extend(occurrences)

                results[unit["uuid"]] = {
                    "uuid": unit["uuid"],
                    "unidad": unit["name"],
                    "jurisdiccion": unit["jurisdiction"],
                    "ocurrencias": all_occurrences
                }
        self.occurrences_results[dimension_name] = results

    def __preprocess_prompt_by_unit(self, prompt, cluster_units, dimension_name) -> None:
        results = {}
        for unit in tqdm(cluster_units, total=len(cluster_units), desc=dimension_name, leave=False):
            try:
                del unit["path"]
            except KeyError:
                pass

            try:
                results[unit["uuid"]] = self.results_to_reuse[dimension_name][unit["uuid"]][0]['unidad_1']
            except (KeyError, IndexError):
                results[unit["uuid"]] = self.__send_prompt(prompt, unit)
        self.occurrences_results[dimension_name] = results

    def __without_duplicates(self, occurrences):
        tags = []
        result = []
        for each in occurrences:
            if each["tag"] not in tags:
                tags.append(each["tag"])
                result.append(each)
        return result

    def __match_reusing_between(self, unit1, unit2, dimension_name):
        try:
            dyads = self.results_to_reuse[dimension_name][unit1["uuid"]]
            for dyad in dyads:
                if dyad["unidad_2"]["uuid"] == unit2["uuid"]:
                    return dyad
        except KeyError:
            return self.__match_between(unit1, unit2, dimension_name)
        return self.__match_between(unit1, unit2, dimension_name)

    def __match_between(self, unit1, unit2, dimension_name):
        result1 = self.occurrences_results[dimension_name][unit1["uuid"]]
        result2 = self.occurrences_results[dimension_name][unit2["uuid"]]
        occurrences1 = self.__without_duplicates(result1["ocurrencias"])
        occurrences2 = self.__without_duplicates(result2["ocurrencias"])
        result1["ocurrencias"] = occurrences1
        result2["ocurrencias"] = occurrences2
        final_result = {
            "unidad_1": result1,
            "unidad_2": result2,
            "matches": [],
        }
        if len(occurrences1) > 0 and len(occurrences2) > 0:
            similarities = self.__compute_similarities(occurrences1, occurrences2)

            for i, occurrence1 in enumerate(occurrences1):
                for j, occurrence2 in enumerate(occurrences2):
                    final_result["matches"].append({
                        "uuid_1": unit1["uuid"],
                        "id_ocurrencia_1": occurrence1["id"],
                        "uuid_2": unit2["uuid"],
                        "id_ocurrencia_2": occurrence2["id"],
                        "cosine_similarity": similarities[i, j],
                    })
        return final_result

    # Proceso principal
    def execute(
        self,
        cluster_id: int,
        reuse: List[Literal[
            DimensionName.OBJECTIVE,
            DimensionName.TARGET,
            DimensionName.ENVIRONMENT,
        ]] = [],
    ):
        cluster_units = units_on_cluster(self.clusters_json, cluster_id)
        total = len(cluster_units)

        existing_file = None
        if reuse != []:
            with open(f"heatmaps/{self.clusters_file_name}_id_{cluster_id}.json", "r", encoding="utf-8") as f:
                existing_file = json.load(f)

        for dimension_name in LLM_BASED_DIMENSIONS:
            if dimension_name in reuse and existing_file is not None:
                self.results_to_reuse[dimension_name.value] = existing_file[dimension_name.value]
            
            if dimension_name == DimensionName.ENVIRONMENT:
                self.__preprocess_environment_dimension_by_unit(cluster_units)
            else:
                self.__preprocess_prompt_by_unit(
                    self.__open_prompt(dimension_name.value),
                    cluster_units,
                    dimension_name=dimension_name.value,
                )

        results = {
            DimensionName.OBJECTIVE.value: defaultdict(list),
            DimensionName.TARGET.value: defaultdict(list),
            DimensionName.ENVIRONMENT.value: defaultdict(list),
        }

        for i in tqdm(
            range(total),
            desc="Filas",
            total=total,
            leave=False,
        ):
            for j in tqdm(
                range(i + 1, total),
                desc="Columnas",
                total=total-(i+1),
                leave=False,
            ):
                u = cluster_units[i]
                v = cluster_units[j]
                for dimension_name in LLM_BASED_DIMENSIONS:
                    results[dimension_name.value][u["uuid"]].append(
                        self.__match_reusing_between(u, v, dimension_name.value))

        with open(f"heatmaps/{self.clusters_file_name}_id_{cluster_id}.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
