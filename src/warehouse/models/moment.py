# remindo_dwh_classes
from sqlalchemy import Date, String, Integer, Boolean, Column, DateTime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from src.warehouse.base import Base
from src.warehouse.models.study import Study
from src.warehouse.models.recipe import Recipe


class Moment(Base):
    __tablename__ = "moments"
    __table_args__ = {"schema": "staging_schema"}

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    caesura = Column(String(1000))
    code = Column(String(200))
    study_name = Column(String(100))
    datasource_id = Column(Integer)
    date_end = Column(String(50))
    date_start = Column(String(50))
    duration = Column(Integer)
    extra_time = Column(String(50))
    limit_ips = Column(Boolean)
    recipe_type = Column(String(50))
    requires_approval = Column(Boolean)
    time_end = Column(String(50))
    time_start = Column(String(50))
    type = Column(String(50))
    status = Column(String(10))
    show_result = Column(String(500))
    show_result_date = Column(String(10))
    show_result_time = Column(String(10))
    show_result_delay = Column(Integer)
    show_result_delay_type = Column(String(10))

    apicall_from = Column(String(10))
    apicall_recipe_id = Column(Integer)

    study = relationship(
        "Study",
        back_populates="moments",
    )
    study_id = Column(Integer, ForeignKey(Study.id, ondelete="CASCADE"))

    recipe = relationship(
        "Recipe",
        back_populates="moments",
    )
    recipe_id = Column(Integer, ForeignKey(Recipe.id, ondelete="CASCADE"))

    moment_results = relationship(
        "MomentResult",
        back_populates="moment",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    reliability = relationship(
        "Reliability",
        back_populates="moment",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    stats = relationship("Stat", cascade="all, delete-orphan")
    items = relationship("Item", cascade="all, delete-orphan")

    extract_date = Column(DateTime, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return (
            "<Moment(id='%s', name='%s', \
            code='%s', extract_date='%s', job_run_id='%s')>"
            % (self.id, self.name, self.code, self.extract_date, self.job_run_id)
        )
