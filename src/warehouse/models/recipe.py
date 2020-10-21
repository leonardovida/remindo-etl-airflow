from sqlalchemy import Date, Float, String, Integer, Boolean, Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from src.warehouse.base import Base
from src.warehouse.models.study import Study


class Recipe(Base):
    __tablename__ = "recipes"
    __table_args__ = {"schema": "staging_schema"}

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    code = Column(String(200))
    category = Column(String(50))
    status = Column(String(50))
    type = Column(String(50))
    max_retries = Column(Integer)
    exam_duration = Column(Integer)
    tools = Column(String(100))

    # TODO: These are to test out
    practice_repeat_until = Column(Integer)
    practice_continue_practice = Column(String(50))
    practice_start_retry_by_candidate = Column(String(50))
    practice_start_retry_delay = Column(Integer)
    exam_caesura = Column(String(1000))
    exam_round_grade_decimals = Column(String(50))
    show_result_given_answer = Column(String(10))
    show_result_correct_answer = Column(String(10))
    show_result_score = Column(String(10))
    show_grade = Column(String(10))
    show_correct = Column(String(10))
    passed = Column(String(10))
    exam_round_grade_decimals = Column(String(10))
    bonuspoints = Column(Boolean)
    extra_time = Column(Boolean)

    # TODO: If null there are problems, but there shoulnd't be
    apicall_since = Column(Float)
    apicall_study_id = Column(String(50))
    apicall_full = Column(String(10))
    apicall_recipe_id = Column(Integer)

    # TODO: test out study_id = Column(Integer, ForeignKey('studies.id'))
    study = relationship("Study", back_populates="recipes")
    study_id = Column(Integer, ForeignKey(Study.id, ondelete="CASCADE"))

    moments = relationship(
        "Moment",
        back_populates="recipe",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    moments_results = relationship(
        "MomentResult",
        back_populates="recipe",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    reliabilities = relationship(
        "Reliability",
        back_populates="recipe",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    stats = relationship("Stat", cascade="all, delete")
    items = relationship("Item", cascade="all, delete")

    extract_date = Column(Date, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return (
            "<Recipe(id='%s', name='%s', \
            code='%s', extract_date='%s', job_run_id='%s')>"
            % (self.id, self.name, self.code, self.extract_date, self.job_run_id)
        )