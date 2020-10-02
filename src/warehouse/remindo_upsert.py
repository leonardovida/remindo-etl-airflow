import configparser
from pathlib import Path
import logging
import warnings

from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect
from sqlalchemy import *


config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

staging_schema = config.get("STAGING", "SCHEMA")
warehouse_schema = config.get("WAREHOUSE", "SCHEMA")


def upsert(engine, session, conn, table_name):
    """Upsert values from staging to warehouse"""
    name = str(table_name.__tablename__)
    logging.debug(f"Performing upsert on table {name}")

    # Select table in staging schema
    metadata_staging = MetaData(schema="staging_schema")
    metadata_staging.bind = engine
    table_staging = Table(
        "clusters", metadata_staging, schema="staging_schema", autoload=True
    )
    # Find the columns for the table in staging
    cols_table_staging = [c for c in table_staging.c]

    # Select table in warehouse schema
    metadata_warehouse = MetaData(schema="warehouse")
    metadata_warehouse.bind = engine
    table_warehouse = Table(
        "clusters", metadata_warehouse, schema="warehouse", autoload=True
    )
    # Find the columns for the table in warehouse
    cols_table_warehouse = [c for c in table_warehouse.c]

    delete_from(table_warehouse, table_staging, session)

    sel = select(cols_table_staging)
    ins = table_warehouse.insert().from_select(cols_table_warehouse, sel)
    session.execute(ins)
    session.commit()


def delete_from(tab1, tab2, session):
    """Delete rows from tab1 using the matching pk from tab2"""
    # Find primary keys
    # tab1_pk = [pk.key for pk in inspect(tab1).primary_key]
    # tab2_pk = [pk.key for pk in inspect(tab2).primary_key]

    # Create delete query
    delete = tab1.delete().where(tab1.c.id == tab2.c.id)
    session.execute(delete)
    session.commit()


# Adapted from: https://stackoverflow.com/questions/41724658/how-to-do-a-proper-upsert-using-sqlalchemy-on-postgresql
def upsert_2(engine, schema, table_name, records=[]):

    logging.debug(f"Performing upsert on table {table_name}")

    metadata = MetaData(schema=schema)
    metadata.bind = engine

    table = Table(table_name, metadata, schema=schema, autoload=True)

    # get list of fields making up primary key
    primary_keys = [key.name for key in inspect(table).primary_key]

    # assemble base statement
    stmt = postgresql.insert(table).values(records)

    # define dict of non-primary keys for updating
    update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

    # cover case when all columns in table comprise a primary key
    # in which case, upsert is identical to 'on conflict do nothing.
    if update_dict == {}:
        warnings.warn("no updateable columns found for table")
        # we still wanna insert without errors
        # insert_ignore(table_name, records)
        return None

    # assemble new statement with 'on conflict do update' clause
    update_stmt = stmt.on_conflict_do_update(
        index_elements=primary_keys,
        set_=update_dict,
    )

    # execute
    with engine.connect() as conn:
        result = conn.execute(update_stmt)
        return result
