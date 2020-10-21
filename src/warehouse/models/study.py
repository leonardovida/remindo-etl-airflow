# remindo_dwh_classes
from sqlalchemy import Date, String, Integer, Boolean, Column, DateTime
from sqlalchemy.orm import relationship
from src.warehouse.base import Base


class Study(Base):
    __tablename__ = "studies"
    __table_args__ = {"schema": "staging_schema"}

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    code = Column(String(200))
    descr = Column(String(200))
    edition_name = Column(String(200))
    edition_descr = Column(String(200))
    source_edition_id = Column(Integer)
    source_study_id = Column(Integer)
    apicall_complete = Column(Boolean)
    apicall_since = Column(Date)

    # cluster_id = Column(Integer, foreign_key=True, nullable=False)
    recipes = relationship(
        "Recipe",
        back_populates="study",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    moments = relationship(
        "Moment",
        back_populates="study",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    moments_results = relationship(
        "MomentResult",
        back_populates="study",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    extract_date = Column(DateTime, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return (
            "<Study(id='%s', name='%s', \
            code='%s', extract_date='%s', job_run_id='%s')>"
            % (self.id, self.name, self.code, self.extract_date, self.job_run_id)
        )
