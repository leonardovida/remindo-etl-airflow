# remindo_dwh_classes
from time import time
from datetime import datetime
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import Sequence

from .remindo_dwh_base import Base

class Cluster(Base):
    __tablename__ = 'clusters'
    __table_args__ = {'schema': 'staging_schema'}

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    load_date = Column(Date)
    job_run_id = Column(Integer)
    
    def __repr__(self):
        return "<Study(id='%s', name='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.name,
            self.load_date,
            self.job_run_id
        )


class Study(Base):
    __tablename__ = 'studies'
    __table_args__ = {'schema': 'staging_schema'}

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    code = Column(String(200))
    descr = Column(String(200))
    edition_name = Column(String(200))
    edition_descr = Column(String(200))
    source_edition_id = Column(Integer)
    source_study_id = Column(Integer)
    #cluster_id = Column(Integer, foreign_key=True, nullable=False)
    api_call_params_complete = Column(Boolean)
    api_call_params_since = Column(Date)
    
    recipes = relationship("Recipe", back_populates='study')
    moments = relationship("Moment", back_populates='study')

    record_create_timestamp = Column(Date, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Study(id='%s', name='%s', code='%s', record_create_timestamp='%s')>" % (
            self.id,
            self.name,
            self.code,
            self.record_create_timestamp
            # self.job_run_id
        )

class Recipe(Base):
    __tablename__ = 'recipes'
    __table_args__ = {'schema': 'staging_schema'}
    
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    code = Column(String(200))
    category = Column(String(50))
    status = Column(String(50))
    type = Column(String(50))
    max_retries = Column(Integer)
    exam_duration = Column(Integer)
    tools = Column(String(100))
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
    api_call_params_since = Column(Float)
    
    # If null there are problems
    api_call_params_study_id = Column(String(50))
    api_call_params_full = Column(String(10))

    # To delete
    recipe_id = Column(Integer)
    study_name = Column(String(200))

    study = relationship("Study", back_populates='recipes')
    moments = relationship("Moment", back_populates='recipe')
    moments_results = relationship("MomentResult", back_populates='recipe')
    reliabilities = relationship("Reliability", back_populates='recipe')
    
    stats = relationship("Stat")
    items = relationship("Item")

    # Do I have to do something with this?
    study_id = Column(Integer, ForeignKey(Study.id))

    load_date = Column(String(50))
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Recipe(id='%s', name='%s', code='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.name,
            self.code,
            self.load_date,
            self.job_run_id
        )

class Moment(Base):
    __tablename__ = 'moments'
    __table_args__ = {'schema': 'staging_schema'}
    
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(200))
    caesura = Column(String(1000))
    code = Column(String(200))
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

    api_call_params_from = Column(String(10))
    api_call_recipe_id = Column(Integer)
    
    study = relationship("Study", back_populates='moments')
    recipe = relationship("Recipe", back_populates='moments')
    moment_results = relationship("MomentResult", uselist=False, back_populates='moment')
    reliability = relationship("Reliability", uselist=False, back_populates='moment')
    stats = relationship("Stat")
    items = relationship("Item")

    study_id = Column(Integer, ForeignKey(Study.id))
    study_name = Column(String(100))
    recipe_id = Column(Integer, ForeignKey(Recipe.id))

    load_date = Column(Date, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment(id='%s', name='%s', code='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.name,
            self.code,
            self.load_date,
            self.job_run_id
        )

class MomentResult(Base):
    __tablename__ = 'moments_results'
    __table_args__ = {'schema': 'staging_schema'}
    
    id = Column(Integer, primary_key=True, nullable=False)
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

    recipe = relationship("Recipe", back_populates='moments_results')
    moment = relationship("Moment", back_populates='moment_results')

    recipe_id = Column(Integer, ForeignKey(Recipe.id))
    moment_id = Column(Integer, ForeignKey(Moment.id))

    load_date = Column(Date, nullable=False)
    job_run_id = Column(Integer)


    def __repr__(self):
        return "<Moment(id='%s', moment_id='%s', recipe_id='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.moment_id,
            self.recipe_id,
            self.load_date,
            self.job_run_id
        )

class Reliability(Base):
    __tablename__ = 'reliability'
    __table_args__ = {'schema': 'staging_schema'}

    reliability_id_seq = Sequence('reliability_id_seq', metadata=Base.metadata)
    id = Column(
        Integer, reliability_id_seq,
        server_default=reliability_id_seq.next_value(), primary_key=True)
    alpha = Column(Float)
    sem = Column(Float)
    notes = Column(String(50))
    missing_count = Column(Integer)
    answer_count = Column(Integer)
    stdev = Column(Float)
    average = Column(Float)
    max = Column(Integer)

    recipe = relationship("Recipe", back_populates='reliabilities')
    moment = relationship("Moment", back_populates='reliability')

    recipe_id = Column(Integer, ForeignKey(Recipe.id))
    moment_id = Column(Integer, ForeignKey(Moment.id))

    load_date = Column(Date, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment(id='%s', recipe_id='%s', moment_id='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.recipe_id,
            self.moment_id,
            self.load_date,
            self.job_run_id
        )


class Stat(Base):
    __tablename__ = 'stats'
    __table_args__ = {'schema': 'staging_schema'}

    stat_id_seq = Sequence('stat_id_seq', metadata=Base.metadata)
    id = Column(
        Integer, stat_id_seq,
        server_default=stat_id_seq.next_value(), primary_key=True)
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

    recipe_id = Column(Integer, ForeignKey(Recipe.id))
    moment_id = Column(Integer, ForeignKey(Moment.id))

    load_date = Column(Date, nullable=False)
    job_run_id = Column(Integer)

    def __repr__(self):
        return "<Moment(id='%s', recipe_id='%s', moment_id='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.recipe_id,
            self.moment_id,
            self.load_date,
            self.job_run_id
        )

class Item(Base):
    __tablename__ = 'items'
    __table_args__ = {'schema': 'staging_schema'}

    item_id_seq = Sequence('item_id_seq', metadata=Base.metadata)
    id = Column(
        Integer, item_id_seq,
        server_default=item_id_seq.next_value(), primary_key=True)
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
    subscription_id = Column(Integer, nullable=False)
    position_item = Column(Integer)
    response_cardinality = Column(String(20))
    response_baseType = Column(String(20))
    response_choiceSequence = Column(String(200))
    response_candidateResponse = Column(Text)
    response_correctResponse = Column(Text)

    recipe_id = Column(Integer, ForeignKey(Recipe.id))
    moment_id = Column(Integer, ForeignKey(Moment.id))

    load_date = Column(Date)
    job_run_id = Column(Integer)
    
    def __repr__(self):
        return "<Moment(id='%s', item_identifier='%s', moment_id='%s', recipe_id='%s', load_date='%s', job_run_id='%s')>" % (
            self.id,
            self.item_identifier,
            self.moment_id,
            self.recipe_id,
            self.load_date,
            self.job_run_id
        )

# Table('association', Base.metadata,
#     Column('items_item_id', String(50), ForeignKey('stats.item_identifier')),
#     Column('stats_item_id', String(50), ForeignKey('items.item_identifier'))
# )