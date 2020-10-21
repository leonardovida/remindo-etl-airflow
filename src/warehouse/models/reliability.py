from sqlalchemy import Date, Float, String, Integer, Column, DateTime, BIGINT
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from src.warehouse.base import Base
from src.warehouse.models.moment import Moment
from src.warehouse.models.recipe import Recipe


class Reliability(Base):
    __tablename__ = 'reliabilities'
    __table_args__ = {'schema': 'staging_schema'}

    # TODO: sequence on Oracles
    id = Column(Integer, primary_key=True, nullable=False)
    alpha = Column(Float)
    sem = Column(Float)
    notes = Column(String(50))
    missing_count = Column(Integer)
    answer_count = Column(Integer)
    stdev = Column(Float)
    average = Column(Float)
    max = Column(Float)

    recipe = relationship(
        "Recipe",
        back_populates='reliabilities',
        cascade="all, delete")
    recipe_id = Column(Integer, ForeignKey(Recipe.id, ondelete="CASCADE"))

    moment = relationship(
        "Moment",
        back_populates='reliability')
    moment_id = Column(Integer, ForeignKey(Moment.id, ondelete="CASCADE"))

    extract_date = Column(String, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment(id='%s', recipe_id='%s', \
            moment_id='%s', extract_date='%s', job_run_id='%s')>" % (
            self.id,
            self.recipe_id,
            self.moment_id,
            self.extract_date,
            self.job_run_id
        )