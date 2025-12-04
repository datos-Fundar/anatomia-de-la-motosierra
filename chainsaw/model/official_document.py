import datetime
from typing import Optional, List
from sqlalchemy.types import JSON
from sqlalchemy import (
    Text,
    Date,
    String,
    Integer,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship
from chainsaw.db import Base
from chainsaw.model.node import Unit


class Objective(Base):
    __tablename__ = "objectives"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(String)
    urls: Mapped[str] = mapped_column(String)
    prompt_id: Mapped[int] = mapped_column(ForeignKey("prompts.id"), nullable=False)
    prompt = relationship("Prompt", back_populates="objective")


class Prompt(Base):
    __tablename__ = "prompts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    urls: Mapped[str] = mapped_column(String, nullable=False)
    unit_uuid: Mapped[str] = mapped_column(String(36), index=True)
    tree_id: Mapped[int] = mapped_column(ForeignKey("trees.id"), nullable=False)
    tree = relationship("Tree", back_populates="prompts")
    objective: Mapped[Objective] = relationship(
        "Objective",
        back_populates="prompt",
        cascade="all, delete-orphan"
    )


class ScrappedBlock(Base):
    __tablename__ = "scrapped_blocks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    scrapped_document_id: Mapped[int] = mapped_column(ForeignKey("scrapped_documents.id"))
    text: Mapped[str] = mapped_column(Text, nullable=False)
    unit_uuid: Mapped[str] = mapped_column(String(36), index=True)
    scrapped_document = relationship("ScrappedDocument", back_populates="scrapped_blocks")


class ScrappedDocument(Base):
    __tablename__ = "scrapped_documents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    official_document_id: Mapped[int] = mapped_column(ForeignKey("official_documents.id"))
    url: Mapped[str] = mapped_column(String, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    official_document = relationship("OfficialDocument", back_populates="scrapped_documents")
    scrapped_blocks: Mapped[ScrappedBlock] = relationship(
        "ScrappedBlock",
        back_populates="scrapped_document",
        cascade="all, delete-orphan"
    )


class OfficialDocument(Base):
    __tablename__ = "official_documents"
    __table_args__ = (
        UniqueConstraint("url", "tree_id", name="uq_official_document_url_tree"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String, index=True)
    related_unit_uuids: Mapped[List[str]] = mapped_column(
        MutableList.as_mutable(JSON),
        default=list,
        nullable=False
    )
    scrapped_documents: Mapped[List["ScrappedDocument"]] = relationship(
        "ScrappedDocument",
        back_populates="official_document",
        cascade="all, delete-orphan"
    )
    processed: Mapped[bool] = mapped_column(default=False, nullable=False)
    tree_id: Mapped[int] = mapped_column(ForeignKey("trees.id"), nullable=False)
    tree = relationship("Tree", back_populates="official_documents")

    @classmethod
    def get(
        cls,
        url: str,
        tree_id: int,
        related_to: str,
        session,
    ) -> "OfficialDocument":
        document = session.query(cls).filter(
            cls.url == url,
            cls.tree_id == tree_id,
            cls.related_unit_uuids.contains(related_to)
        ).first()
        if document:
            return document

        document = session.query(cls).filter(
            cls.url == url,
            cls.tree_id == tree_id,
        ).first()
        if document:
            if related_to not in document.related_unit_uuids:
                document.related_unit_uuids.append(related_to)
                session.add(document)
        else:
            document = cls(
                url=url,
                tree_id=tree_id,
                related_unit_uuids=[related_to],
            )
            session.add(document)
            session.flush()

            similar_document = session.query(cls).filter(
                cls.url == url,
            ).first()
            if similar_document:
                for already_scrapped in similar_document.scrapped_documents:
                    scrapped = ScrappedDocument(
                        official_document_id=document.id,
                        url=already_scrapped.url,
                        text=already_scrapped.text,
                        date=already_scrapped.date,
                    )
                    session.add(scrapped)
        session.flush()
        return document
