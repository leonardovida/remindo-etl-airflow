from sqlalchemy import Date, String, Integer, Column, DateTime
from src.warehouse.base import Base


class Cluster(Base):
    """Database class for clusters table"""

    __tablename__ = "clusters"
    __table_args__ = {"schema": "staging_schema"}

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    extract_date = Column(DateTime)
    job_run_id = Column(Integer)

    def __repr__(self):
        return (
            "<Study(id='%s', name='%s', \
            load_date='%s', job_run_id='%s')>"
            % (self.id, self.name, self.extract_date, self.job_run_id)
        )
