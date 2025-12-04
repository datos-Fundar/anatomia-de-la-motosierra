from sqlalchemy import String
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column
from chainsaw.db import Base
from chainsaw.enum.unit_status import UnitStatus


class TreeChange(Base):
    __tablename__ = "tree_changes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    unit_name: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[UnitStatus] = mapped_column(
        SQLEnum(UnitStatus, values_callable=lambda enum: [e.value for e in enum], name="unitstatus"),
        nullable=False
    )
    uuid_2023: Mapped[str | None] = mapped_column(String(36), nullable=True)
    uuid_2025: Mapped[str | None] = mapped_column(String(36), nullable=True)
    path_2023: Mapped[str | None] = mapped_column(String, nullable=True)
    path_2025: Mapped[str | None] = mapped_column(String, nullable=True)
