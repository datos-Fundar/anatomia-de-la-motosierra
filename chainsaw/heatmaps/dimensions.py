from abc import ABC, abstractmethod
from typing import List, Dict, Any, Type
from suitable_class_finder import SuitableClassFinder
from networkx.algorithms.lowest_common_ancestors import lowest_common_ancestor
from chainsaw.db import SessionLocal
from chainsaw.model.tree import Tree
from chainsaw.heatmaps.constants import DimensionName


class Dimension(ABC):
    DIMENSION_NAME: DimensionName

    @classmethod
    def working_on(cls, dimension_name) -> Type["Dimension"]:
        return SuitableClassFinder(cls).suitable_for(dimension_name)

    @classmethod
    def can_handle(cls, dimension_name: str) -> bool:
        return cls.DIMENSION_NAME == dimension_name

    def __init__(
        self,
        results: Dict[str, Any],
        tree_file: str = "",
        central_administration_only: bool = True,
        threshold: float = 0.8,
    ) -> None:
        self.results = results
        self.tree_file = tree_file
        self.central_administration_only = central_administration_only
        self.threshold = threshold

    @abstractmethod
    def partial_matrix(self, units_order) -> List[List[float]]:
        pass


class DistanceDimension(Dimension):
    DIMENSION_NAME = DimensionName.DISTANCE

    @classmethod
    def __generate_root_distances(cls, units_order, tree) -> Dict[str, int]:
        root_distances = {}
        for unit_uuid in units_order.keys():
            path = tree.uuid_path_to(unit_uuid)
            for uuid in path:
                try:
                    root_distances[uuid]
                except KeyError:
                    root_distances[uuid] = len(tree.uuid_path_to(uuid))
        return root_distances

    @classmethod
    def __path_distance(cls, tree, row_uuid, column_uuid, root_distances) -> float:
        parent_uuid = lowest_common_ancestor(tree.graph, row_uuid, column_uuid)
        if parent_uuid in (row_uuid, column_uuid):
            return 0.0

        row_distance = root_distances[row_uuid]
        column_distance = root_distances[column_uuid]
        parent_distance = root_distances[parent_uuid]
        length = (row_distance - parent_distance) + (column_distance - parent_distance)
        deepest_uuid = max(row_distance, column_distance)
        return length / deepest_uuid

    def partial_matrix(self, units_order) -> List[List[float]]:
        total = len(units_order)
        units_inverted = {unit_data["idx"]: uuid for uuid, unit_data in units_order.items()}
        matrix = [[0.0 for j in range(total)] for i in range(total)]

        with SessionLocal() as session:
            tree = Tree.load_or_create(
                self.tree_file,
                session,
                central_administration_only=self.central_administration_only,
            )
            root_distances = self.__generate_root_distances(units_order, tree)

            for row_idx in range(total):
                for column_idx in range(row_idx + 1, total):
                    value = self.__path_distance(
                        tree,
                        units_inverted[row_idx],
                        units_inverted[column_idx],
                        root_distances,
                    )
                    matrix[row_idx][column_idx] = value
                    matrix[column_idx][row_idx] = value
        return matrix


class AbstractLLMBasedDimension(Dimension):
    @property
    def dimension_results(self) -> Dict[str, Any]:
        return self.results[self.DIMENSION_NAME]

    def partial_matrix(self, units_order) -> List[List[float]]:
        total = len(units_order)
        matrix = [[0.0 for j in range(total)] for i in range(total)]

        for unit_uuid, units_data in self.dimension_results.items():
            row_idx = units_order[unit_uuid]["idx"]
            for unit_data in units_data:
                try:
                    column_idx = units_order[unit_data["unidad_2"]["uuid"]]["idx"]
                    occurrences1_amount = len(unit_data[f"unidad_1"]["ocurrencias"])
                    occurrences2_amount = len(unit_data[f"unidad_2"]["ocurrencias"])
                    if occurrences1_amount >= occurrences2_amount:
                        min_occurrences = occurrences2_amount
                        min_unit = 2
                    else:
                        min_occurrences = occurrences1_amount
                        min_unit = 1
                    
                    sum_matches = 0
                    for idx in range(min_occurrences):
                        similarities = [match["cosine_similarity"]
                                        for match in unit_data["matches"]
                                        if match[f"id_ocurrencia_{min_unit}"] == idx]
                        max_similarity = max(similarities) if similarities else 0
                        if max_similarity >= self.threshold:
                            sum_matches = sum_matches + 1

                    if min_occurrences == 0:
                        partial_index = -1.0
                    elif 0 < min_occurrences <= sum_matches:
                        partial_index = 1.0
                    elif sum_matches == 0 and 0 < min_occurrences:
                        partial_index = 0.0
                    else:
                        partial_index = sum_matches / min_occurrences
                    # partial_index = partial_index * weight
                    matrix[row_idx][column_idx] = partial_index
                    matrix[column_idx][row_idx] = partial_index
                except KeyError as error:
                    print(error)
        return matrix


class ObjectiveDimension(AbstractLLMBasedDimension):
    DIMENSION_NAME = DimensionName.OBJECTIVE


class TargetDimension(AbstractLLMBasedDimension):
    DIMENSION_NAME = DimensionName.TARGET


class EnvironmentDimension(AbstractLLMBasedDimension):
    DIMENSION_NAME = DimensionName.ENVIRONMENT
