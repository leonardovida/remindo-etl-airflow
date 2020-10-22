from sqlalchemy import Float, String, Integer, Boolean, Column, DateTime, Table
from sqlalchemy import ForeignKey, Text
from sqlalchemy.orm import relationship
from src.warehouse.base import Base
# from src.warehouse.models.recipe import Recipe
# from src.warehouse.models.moment import Moment
from src.warehouse.models.Stat import Stat


class Item(Base):
    __tablename__ = 'items'
    __table_args__ = {'schema': 'staging_schema'}

    # TODO: sequence on Oracle
    id = Column(Integer, primary_key=True, nullable=False)
    item_identifier = Column(String(200), nullable=False)
    num_attempts = Column(Integer)
    duration = Column(Integer)
    status = Column(String(100))
    score = Column(Float)
    passed = Column(Boolean)
    max_score = Column(Float)
    flagged = Column(Boolean)
    check_manually = Column(Boolean)
    weight = Column(Integer)
    subscription_id = Column(Integer)
    position_item = Column(Integer)
    response_cardinality = Column(String(20))
    response_baseType = Column(String(20))
    response_choiceSequence = Column(String(200))
    response_candidateResponse = Column(Text)
    response_correctResponse = Column(Text)

    # recipe_id = Column(Integer, ForeignKey(Recipe.id))
    recipe_id = Column(Integer)
    # moment_id = Column(Integer, ForeignKey(Moment.id))
    moment_id = Column(Integer)

    extract_date = Column(DateTime, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment(id='%s', item_identifier='%s', moment_id='%s', \
            recipe_id='%s', extract_date='%s', job_run_id='%s')>" % (
            self.id,
            self.item_identifier,
            self.moment_id,
            self.recipe_id,
            self.extract_date,
            self.job_run_id
        )
