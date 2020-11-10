from sqlalchemy import Float, String, Integer, Column, DateTime
from src.warehouse.base import Base

# from src.warehouse.models.moment import Moment
# from src.warehouse.models.recipe import Recipe
# Need to connect stat and item


class Stat(Base):
    """Database class for statistics table"""

    __tablename__ = 'stats'
    __table_args__ = {'schema': 'staging_schema'}

    # TODO: sequence on Oracle
    id = Column(Integer, primary_key=True, nullable=False)
    item_identifier = Column(String(200), nullable=False)
    code = Column(String(200))
    type = Column(String(50))
    language = Column(String(10))
    max_score = Column(Float)
    interaction_count = Column(Integer)
    difficulty = Column(Integer)
    section = Column(String(50))
    question_position = Column(String(200))
    p = Column(Float)
    std = Column(Float)
    rir = Column(Float)
    total = Column(Integer)
    answered = Column(Integer)

    # recipe_id = Column(Integer, ForeignKey(Recipe.id))
    recipe_id = Column(Integer)
    # moment_id = Column(Integer, ForeignKey(Moment.id))
    moment_id = Column(Integer)

    extract_date = Column(DateTime, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment( \
            id='%s', recipe_id='%s', moment_id='%s', \
                extract_date='%s', job_run_id='%s')>" % (
            self.id,
            self.recipe_id,
            self.moment_id,
            self.extract_date,
            self.job_run_id
        )
