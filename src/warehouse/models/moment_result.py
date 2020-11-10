from sqlalchemy import Date, Float, String, Integer, Boolean, Column, DateTime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from src.warehouse.base import Base
from src.warehouse.models.study import Study
from src.warehouse.models.recipe import Recipe
from src.warehouse.models.moment import Moment


class MomentResult(Base):
    """Database class for moments results table"""
    
    __tablename__ = 'moments_results'
    __table_args__ = {'schema': 'staging_schema'}

    # TODO: id might be result_id
    result_id = Column(Integer, primary_key=True, nullable=False)
    subscription_id = Column(Integer, nullable=False)
    user_id = Column(Integer)
    user_code = Column(String(50))
    status = Column(String(50))
    start_time = Column(Date)
    end_time = Column(Date)
    max_score = Column(Integer)
    score = Column(Integer)
    grade = Column(Float)
    try_count = Column(Integer)
    i_count = Column(Integer)
    i_right = Column(Integer)
    i_answered = Column(Integer)
    i_review = Column(Integer)
    i_correct = Column(Integer)
    i_incorrect = Column(Integer)
    i_mostlycorrect = Column(Integer)
    i_mostlyincorrect = Column(Integer)
    show_given_answer = Column(String(50))
    show_score = Column(Boolean)
    show_correct = Column(Boolean)
    show_grade = Column(Boolean)
    show_passed = Column(Boolean)
    report_data = Column(Boolean)
    passed = Column(Boolean)
    area_name = Column(String(50))
    area_feedback = Column(Boolean)
    score_type = Column(String(50))
    grade_formatted = Column(Boolean)
    can_change = Column(Boolean)

    study = relationship("Study", back_populates='moments_results')
    study_id = Column(Integer, ForeignKey(Study.id, ondelete="CASCADE"))

    recipe = relationship("Recipe", back_populates='moments_results')
    recipe_id = Column(Integer, ForeignKey(Recipe.id, ondelete="CASCADE"))

    moment = relationship("Moment", back_populates='moment_results')
    moment_id = Column(Integer, ForeignKey(Moment.id, ondelete="CASCADE"))

    extract_date = Column(DateTime, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment(result_id='%s', moment_id='%s', \
            recipe_id='%s', extract_date='%s', job_run_id='%s')>" % (
            self.result_id,
            self.moment_id,
            self.recipe_id,
            self.extract_date,
            self.job_run_id
        )
