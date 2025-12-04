from typing import Any, List
from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from chainsaw.db import Base
from chainsaw.enum.field import Field


class Node(Base):
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column()
    type: Mapped[str] = mapped_column()
    uuid: Mapped[str] = mapped_column(String(36), nullable=False)
    tree_id: Mapped[int] = mapped_column(ForeignKey("trees.id"))

    @classmethod
    def get_field(cls, data, field) -> Any:
        value = data[field.value]
        if type(value) == str:
            value = value.strip()
        return value


class Unit(Node):
    __tablename__ = "units"
    __table_args__ = (
        UniqueConstraint("uuid", "tree_id", name="uq_node_uuid_tree_id"),
    )

    unit_class: Mapped[str] = mapped_column(nullable=False)
    charges: Mapped[List["Charge"]] = relationship(back_populates="unit")
    tree: Mapped["Tree"] = relationship(back_populates="units")
    range: Mapped[str] = mapped_column()

    def __init__(self, data: dict, uuid: str, tree_id: int):
        super().__init__()
        self.name = self.get_field(data, Field.unidad)
        self.type = self.get_field(data, Field.tipo_administracion)
        self.unit_class = self.get_field(data, Field.unidad_clase)
        self.range = self.get_field(data, Field.unidad_rango)
        self.uuid = uuid
        self.tree_id = tree_id


class Charge(Node):
    __tablename__ = "charges"

    charge_name: Mapped[str] = mapped_column()
    charge_order: Mapped[int] = mapped_column()
    first_name: Mapped[str] = mapped_column()
    last_name: Mapped[str] = mapped_column()
    reports_to: Mapped[str] = mapped_column()

    unit_id: Mapped[int] = mapped_column(ForeignKey("units.id"))
    unit: Mapped["Unit"] = relationship(back_populates="charges")
    tree: Mapped["Tree"] = relationship(back_populates="charges")

    def __init__(
        self,
        data: dict,
        unit_id: int,
        uuid: str,
        tree_id: int,
    ):
        super().__init__()
        self.charge_name = self.get_field(data, Field.cargo)
        self.charge_order = self.get_field(data, Field.car_orden)
        self.tree_id = tree_id
        self.first_name = self.get_field(data, Field.autoridad_nombre)
        self.last_name = self.get_field(data, Field.autoridad_apellido)
        self.reports_to = self.get_field(data, Field.reporta_a)
        self.type = self.get_field(data, Field.tipo_administracion)
        self.unit_id = unit_id
        self.uuid = uuid
        full_name = f": {self.last_name}, {self.first_name}" if self.last_name else ""
        self.name = f"{self.charge_name} ({self.charge_order}){full_name}"
