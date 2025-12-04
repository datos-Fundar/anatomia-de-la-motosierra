import re
import csv
import uuid
import pandas as pd
import networkx as nx
from typing import Optional, List, Union
from tqdm import tqdm
from copy import copy
from pathlib import Path
from sqlalchemy import String, ForeignKey, Integer, Boolean, UniqueConstraint
from sqlalchemy.orm import (
    Mapped,
    relationship,
    mapped_column,
)

from chainsaw.db import Base
from chainsaw.enum.field import Field
from chainsaw.enum.administration_type import AdministrationType
from chainsaw.model.tree_change import TreeChange
from chainsaw.model.node import Node, Unit, Charge
from chainsaw.model.official_document import OfficialDocument, Objective


class Edge(Base):
    __tablename__ = "edges"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tree_id: Mapped[int] = mapped_column(ForeignKey("trees.id"), nullable=False)
    source: Mapped[int] = mapped_column(Integer, nullable=False)
    target: Mapped[int] = mapped_column(Integer, nullable=False)
    tree = relationship("Tree", back_populates="edges")


class Tree(Base):
    __tablename__ = "trees"
    __table_args__ = (
        UniqueConstraint("path_file", "central_administration_only", name="uq_tree_path_file_central_admin"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    path_file: Mapped[str] = mapped_column(String, unique=False, index=True)
    date_string: Mapped[str] = mapped_column(String, unique=False, index=True)
    root_uuid: Mapped[str] = mapped_column(String(36), unique=False, index=False)
    central_administration_only: Mapped[bool] = mapped_column(Boolean, unique=False, index=False)
    units: Mapped[List[Unit]] = relationship(
        "Unit",
        back_populates="tree",
        cascade="all, delete-orphan",
    )
    charges: Mapped[List[Charge]] = relationship(
        "Charge",
        back_populates="tree",
        cascade="all, delete-orphan",
    )
    edges: Mapped[List[Edge]] = relationship(
        "Edge",
        back_populates="tree",
        cascade="all, delete-orphan",
    )
    prompts: Mapped[List[Objective]] = relationship(
        "Prompt",
        back_populates="tree",
        cascade="all, delete-orphan"
    )
    official_documents: Mapped[List[OfficialDocument]] = relationship(
        "OfficialDocument",
        back_populates="tree",
        cascade="all, delete-orphan"
    )

    ROOT_NAME = "Presidencia de la Nación"

    @property
    def nodes(self):
        return self.units + self.charges

    @property
    def graph(self):
        try:
            __graph__ = self.__graph
            return __graph__
        except AttributeError:
            __graph__ = nx.DiGraph()
            for node in self.nodes:
                __graph__.add_node(node.uuid, node=node)
            for edge in self.edges:
                __graph__.add_edge(edge.source, edge.target)
            self.__graph = __graph__
            return self.__graph

    @classmethod
    def load_or_create(
        cls,
        path_file,
        session,
        central_administration_only: bool = True,
    ) -> "Tree":
        if (existing_tree := session.query(Tree).filter_by(
            date_string=Path(path_file).stem,
            central_administration_only=central_administration_only
        ).first()):
            return existing_tree
        else:
            try:
                tree = cls(
                    path_file=path_file,
                    central_administration_only=central_administration_only,
                )
                session.add(tree)
                session.flush()

                tree.__build_graph(path_file, session)
                tree.units = [
                    node
                    for _, data in tree.graph.nodes(data=True)
                    if (node := data.get("node"))
                    and node.__class__ == Unit
                ]
                tree.charge = [
                    node
                    for _, data in tree.graph.nodes(data=True)
                    if (node := data.get("node"))
                    and node.__class__ == Charge
                ]
                edges = [
                    Edge(source=source, target=target, tree=tree)
                    for source, target in tree.graph.edges()
                ]
                for edge in edges:
                    session.add(edge)
                tree.edges = edges
                session.commit()
                return tree
            except FileNotFoundError:
                raise FileNotFoundError(f"File not found: {path_file}")

    def __init__(
        self,
        path_file: str,
        central_administration_only: bool = True,
    ) -> None:
        self.path_file = path_file
        self.date_string = Path(path_file).stem
        self.central_administration_only = central_administration_only
        self.root_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, self.ROOT_NAME))

    def __known_path_parts(self, data: dict) -> list[str]:
        path = list(dict.fromkeys([
            Unit.get_field(data, Field.jurisdiccion),
            Unit.get_field(data, Field.subjurisdiccion),
            Unit.get_field(data, Field.unidad_de_nivel_politico),
            Unit.get_field(data, Field.unidad),
        ]))
        try:
            path.remove("")
        except KeyError:
            pass
        finally:
            return path

    def __is_jurisdiction(self, data: dict) -> bool:
        # Los nodos jurisdicción (Ministerios, Jefaturas, etc) tienen todos los campos iguales
        return len(self.__known_path_parts(data)) == 1

    def jurisdictions(self):
        jurisdictions = self.graph.successors(self.root_uuid)
        return [self.ROOT_NAME]+[self.as_name(uuid)
                                 for uuid in jurisdictions
                                 if (node := self.graph.nodes[uuid]["node"])
                                 and node.__class__ == Unit]

    def as_name(self, uuid: str) -> str:
        return self.graph.nodes[uuid]["node"].name

    def node_at_uuid(self, uuid: str) -> Node:
        return self.graph.nodes[uuid].get('node')

    def uuid_path_to(self, target: str, source: Optional[str] = None) -> list[str]:
        __source__ = source if source is not None else self.root_uuid
        return nx.shortest_path(self.graph, __source__, target)

    def path_to(self, target: str, source: Optional[str] = None) -> list[str]:
        return [self.as_name(part) for part in self.uuid_path_to(target, source=source)]

    def path_format(self, path: list[str]) -> str:
        return " -> ".join(path)

    def uuid_from_path(self, path: Union[list[str], str], separator: Optional[str] = ' -> ') -> str:
        if isinstance(path, str):
            path = path.split(separator)
        assert path[0] == self.ROOT_NAME
        parent = self.root_uuid
        for part in path:
            names = {self.as_name(uuid): uuid
                     for uuid in self.graph.successors(parent)}
            names[self.as_name(parent)] = parent
            parent = names[part]
        return parent

    def descendant_uuids(self, parent_uuid: str) -> set[str]:
        descendants = set(nx.descendants(self.graph, parent_uuid))
        # La jurisdicción no se agrega a si misma como posible descendiente.
        descendants.add(parent_uuid)
        return descendants

    def all_nodes_named(self, name: str, source: str) -> list[str]:
        descendants = self.descendant_uuids(source)
        return [descendant_uuid
                for descendant_uuid in descendants
                if self.as_name(descendant_uuid) == name]

    def uuid_for(self, data: dict, parent: str, charge: bool = False) -> str:
        path = self.path_to(parent)
        if charge:
            charge_name = Unit.get_field(data, Field.cargo)
            charge_order = Unit.get_field(data, Field.car_orden)
            last_name = Unit.get_field(data, Field.autoridad_apellido)
            first_name = Unit.get_field(data, Field.autoridad_nombre)
            raw_norms = Unit.get_field(data, Field.norma_competencias_objetivos)
            path.append(f"{charge_name} ({charge_order}) [{raw_norms}]: {last_name}, {first_name}")
        else:
            unit_name = Unit.get_field(data, Field.unidad)
            if path[-1] != unit_name:
                path.append(unit_name)
        return str(uuid.uuid5(uuid.NAMESPACE_URL, self.path_format(path)))

    def __build_official_documents(
        self,
        data: dict,
        related_uuid: str,
        session,
    ) -> list[OfficialDocument]:
        raw_norms = Unit.get_field(data, Field.norma_competencias_objetivos)
        raw_norms = re.sub(r';{2,}', ';', raw_norms)
        raw_norms = re.findall(
            r'(https?.*?)(?=\)+\;+\[+|\)\;|\)\[|\)\(|\)\,|\)\:|\)?\s|\)?$|\;\[|\;$|\[|\])',
            raw_norms,
        )

        return [
            OfficialDocument.get(
                url,
                tree_id=self.id,
                related_to=related_uuid,
                session=session)
            for url in raw_norms
        ]

    def __add_node(
        self,
        data: dict,
        session,
        parent: Optional[str],
        _uuid: Optional[str] = None,
    ) -> Unit:
        if not _uuid:
            unit_uuid = self.uuid_for(data, parent)
        else:
            unit_uuid = _uuid

        unit = session.query(Unit).filter(
            Unit.uuid == unit_uuid,
            Unit.tree_id == self.id,
        ).one_or_none()
        if not unit:
            unit = Unit(data, uuid=unit_uuid, tree_id=self.id)
            self.graph.add_node(unit.uuid, node=unit)
            session.add(unit)

        if not _uuid:
            self.graph.add_edge(parent, unit.uuid)
        session.flush()

        self.__build_official_documents(
            data,
            unit.uuid,
            session,
        )
        charge = Charge(
            data,
            unit_id=unit.id,
            uuid=self.uuid_for(data, unit.uuid, charge=True),
            tree_id=self.id,
        )
        self.graph.add_node(charge.uuid, node=charge)
        self.graph.add_edge(unit.uuid, charge.uuid)
        session.add(charge)
        session.flush()
        return unit

    def as_dataframe(self) -> pd.DataFrame:
        rows = []

        for node in self.nodes:
            if node is None:
                continue

            parent_uuid = None
            predecessor_uuids = list(self.graph.predecessors(node.uuid))
            if len(predecessor_uuids) > 1:
                raise AssertionError(f"Too many predecessors for {node.uuid}")
            elif len(predecessor_uuids) == 1:
                parent_uuid = predecessor_uuids[0]
            else:
                parent_uuid = ""

            rows.append({
                "uuid": node.uuid,
                "parent": parent_uuid,
                "name": node.name,
                "path": "<br>".join(self.path_to(node.uuid)),
            })

        df = pd.DataFrame(rows)
        return df

    def __build_graph(self, path_file, session) -> None:
        self.__graph = nx.DiGraph()
        with Path(path_file).open("r", encoding="utf-8") as file:
            total_rows = sum(1 for _ in file) - 2  # Descuento el header y la primer row, Presidencia

        with Path(path_file).open("r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)

            self.__add_node(
                next(reader),
                session=session,
                parent=None,
                _uuid=self.root_uuid,
            )

            current_jurisdiction = self.root_uuid
            for data in tqdm(reader, total=total_rows, desc="Procesando nodos"):
                if (
                    not self.central_administration_only or
                    Unit.get_field(data, Field.tipo_administracion) == AdministrationType.CENTRAL_ADMINISTRATION.value
                ):
                    current_jurisdiction = self.__process_node_data(
                        data,
                        session,
                        current_jurisdiction,
                    )

    def __process_node_data(
        self,
        data: dict,
        session,
        current_jurisdiction: str,
    ) -> str:
        new_jurisdiction = current_jurisdiction
        if self.__is_jurisdiction(data):
            try:
                jurisdiction_uuid = self.uuid_for(data, self.root_uuid)
                self.graph.nodes[jurisdiction_uuid]
            except KeyError:
                unit = self.__add_node(
                    data,
                    session=session,
                    parent=self.root_uuid,
                )
                new_jurisdiction = unit.uuid
        else:
            reports_name = Unit.get_field(data, Field.reporta_a)
            candidates = self.all_nodes_named(
                reports_name,
                source=current_jurisdiction,
            )
            if len(candidates) == 0:
                # Esto no deberia pasar...
                # actualmente solo pasa para el "Consejo Asesor del Sector Privado" (2023), dado que no se encuentra su creación
                # Lo creo manualmente (suponiendo a quien reporta) para luego ponerlo como padre de la unidad actual
                unit_name = "Consejo Asesor del Sector Privado"
                parent_name = "Instituto Nacional de Tecnología Agropecuaria"
                duplicated = copy(data)
                duplicated[Field.unidad.value] = unit_name
                duplicated[Field.reporta_a.value] = parent_name

                grandparent_uuids = self.all_nodes_named(
                    parent_name,
                    source=current_jurisdiction,
                )
                assert len(grandparent_uuids) == 1
                unit = self.__add_node(
                    duplicated,
                    session=session,
                    parent=grandparent_uuids[0],
                )
                self.__add_node(
                    data,
                    session=session,
                    parent=unit.uuid,
                )

                if unit_name != reports_name:
                    # por las dudas, sigo logueando la alerta
                    print(f"There is no candidate for reports_to '{reports_name}' related to node named {Unit.get_field(data, Field.unidad)}")
                return new_jurisdiction
            elif len(candidates) > 1:
                # eventualmente pueden haber varios... busco el que tenga el path mas parecido a la unidad actual.
                paths = {candidate: self.path_to(candidate, source=current_jurisdiction)
                         for candidate in candidates}
                current_path = self.__known_path_parts(data)
                current_path.remove(Unit.get_field(data, Field.unidad))
                candidates = [uuid for uuid, path in paths.items() if path == current_path]
                if len(candidates) == 1:
                    # uno de los paths es igual al esperado
                    candidate_uuid = candidates[0]
                else:
                    # no hay ninguno igual, busco el mas parecido
                    score = {sum([part in path for part in current_path]): uuid
                             for uuid, path in paths.items()}
                    candidate_uuid = score[max(score.keys())]
            else:  # candidato único, escenario ideal
                candidate_uuid = candidates[0]
            self.__add_node(
                data,
                session=session,
                parent=candidate_uuid,
            )
        return new_jurisdiction
