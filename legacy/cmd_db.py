import os
import pickle
from random import sample

import click
import psycopg
from celery import group
from config.definitions import DATA_DATE
from flask import current_app
from flask.cli import AppGroup
from lib.util_cleancsv import CleanCsv, open_db
from sqlalchemy_utils import create_database, database_exists

app = current_app
db_cli = AppGroup("db")


@db_cli.command("init")
@click.argument("postfix")
@click.option(
    "--with-testdb/--no-with-testdb",
    default=True,
    help="Create a test db too?",
)
@click.pass_context
def init(ctx, postfix, with_testdb):
    """
    Link to or create database with "_test" appended to it

    :param with_testdb: Create a test database
    :return: None
    """

    if with_testdb:
        db_uri = app.config["SQLALCHEMY_DATABASE_URI"]

        if not database_exists(db_uri):
            print("creating database")
            create_database(db_uri)

    # db.drop_all()
    # ctx.invoke(drop_all)
    # db.create_all()
    ctx.invoke(create, postfix)  # Create tables from SQL

    return None


@db_cli.command("reset")
@click.option(
    "--with-testdb/--no-with-testdb",
    default=True,
    help="Create a test db too?",
)
@click.pass_context
def reset(ctx, with_testdb):
    """
    Init and seed automatically.

    :param with_testdb: Create a test database
    :return: None
    """
    ctx.invoke(init, with_testdb=with_testdb)
    ctx.invoke(seed)

    return None


@db_cli.command("drop_columns")
@click.option(
    "--stage-db/--bklg-db",
    default=True,
    help="Drop columns in the stage or dev database?",
)
def drop_columns(stage_db):
    if stage_db:
        conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
        conn = psycopg.connect(conn_string)

    to_drop = {
        f"foia_case_{DATA_DATE}": [
            "alien_city",
            "alien_zipcode",
            "updated_zipcode",
            "updated_city",
            "c_birthdate",
        ]
    }
    with open_db(conn) as curs:
        for table, columns in to_drop.items():
            for column in columns:
                curs.execute(f"ALTER TABLE {table} DROP COLUMN {column}")


@db_cli.command("create_all")
def create_all_tx():
    """
    Create all tables from bklg.blueprints.manager.tx
    Including those not currently included in prod server
    """
    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)
    with open_db(conn) as curs:
        for tx_name, create in create_tx_functions.items():
            create(curs)
            click.echo(f"Created {tx_name} table")


@db_cli.command("create")
@click.argument("postfix", type=str)
def create(postfix):
    """
    Create only those tables needed for the prod server.
    """
    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)
    with open_db(conn) as curs:
        tables = open("/app/sql-scripts/foia_tables.sql", "r").read()
        tables = tables.replace("(%s)", postfix)
        curs.execute(tables)


@db_cli.command("index")
@click.argument("postfix", type=str)
def create(postfix):
    """
    Create only those tables needed for the prod server.
    """
    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)
    with open_db(conn) as curs:
        file = open(f"/app/sql-scripts/post_copy.sql", "r").read()
        file = file.replace("(%s)", postfix)
        curs.execute(file)


@db_cli.command("drop_foia")
@click.argument("postfix")
def drop_foia(postfix):
    """
    Drop all tables and cascade.
    """
    if not click.confirm("Are you sure you want to drop all tables?"):
        return

    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)
    with open_db(conn) as curs:
        curs.execute(
            f"""
            DROP TABLE "foia_appeal_{postfix}";
            DROP TABLE "foia_application_{postfix}";
            DROP TABLE "foia_bond_{postfix}";
            DROP TABLE "foia_charges_{postfix}";
            DROP TABLE "foia_motion_{postfix}";
            DROP TABLE "foia_rider_{postfix}";
            DROP TABLE "foia_schedule_{postfix}";
            DROP TABLE "foia_proceeding_{postfix}";
            DROP TABLE "foia_case_{postfix}";
            """
        )


@db_cli.command("copy")
@click.argument("path", type=click.Path(exists=True))
def copy(path, table: str):
    """
    Copy cleaned file to the database.
    """
    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)

    with open_db(conn) as curs:
        with open(path) as f:
            next(f)  # skip header
            curs.copy_from(f, table, sep="\t", size=8192)


@db_cli.command("alter")
@click.option("--prefix")
def alter(prefix):
    """
    Copy cleaned file to the database.
    """
    conn_string = app.config["SQLALCHEMY_DATABASE_URI"].replace("+psycopg", "")
    conn = psycopg.connect(conn_string)

    if not prefix:
        prefix = DATA_DATE

    with open_db(conn) as curs:
        curs.execute(
            f"""
                ALTER TABLE foia_bond RENAME TO "foia_bond_{prefix}";
                ALTER TABLE foia_appeal RENAME TO "foia_appeal_{prefix}";
                ALTER TABLE foia_application RENAME TO "foia_application_{prefix}";
                ALTER TABLE foia_rider RENAME TO "foia_rider_{prefix}";
                ALTER TABLE foia_motion RENAME TO "foia_motion_{prefix}";
                ALTER TABLE foia_charges RENAME TO "foia_charges_{prefix}";
                ALTER TABLE foia_schedule RENAME TO "foia_schedule_{prefix}";
                ALTER TABLE foia_proceeding RENAME TO "foia_proceeding_{prefix}";
                ALTER TABLE foia_case RENAME TO "foia_case_{prefix}";
                ALTER INDEX ix_foia_appeal_idncase RENAME TO "ix_foia_appeal_idncase_{prefix}";
                ALTER INDEX ix_foia_appln_idncase RENAME TO "ix_foia_appln_idncase_{prefix}";
                ALTER INDEX ix_foia_bond_idncase RENAME TO "ix_foia_bond_idncase_{prefix}";
                ALTER INDEX ix_foia_case_idncase RENAME TO "ix_foia_case_idncase_{prefix}";
                ALTER INDEX ix_foia_charges_idncase RENAME TO "ix_foia_charges_idncase_{prefix}";
                ALTER INDEX ix_foia_motion_idncase RENAME TO "ix_foia_motion_idncase_{prefix}";
                ALTER INDEX ix_foia_proceeding_idncase RENAME TO "ix_foia_proceeding_idncase_{prefix}";
                ALTER INDEX ix_foia_lead_idncase RENAME TO "ix_foia_lead_idncase_{prefix}";
                ALTER INDEX ix_foia_rider_idncase RENAME TO "ix_foia_rider_idncase_{prefix}";
                ALTER INDEX ix_foia_schedule_idncase RENAME TO "ix_foia_schedule_idncase_{prefix}";
                """
        )


@db_cli.command("totable")
@click.argument("path")
@click.argument("postfix")
@click.option("--choose", default=False, is_flag=True)
def copy_to_table(path, postfix, choose):
    """
    Interactively copy files from FOIA to the database.
    """
    file_basenames = [
        "A_TblCase.csv",
        "B_TblProceeding.csv",
        "D_TblAssociatedBond.csv",
        "tblAppeal.csv",
        "tbl_Court_Appln.csv",
        "tbl_Court_Motions.csv",
        "tbl_Lead_Rider.csv",
        "B_TblProceedCharges.csv",
        "tbl_schedule.csv",
    ]

    if choose:
        choices = []
        for file_name in file_basenames:
            if click.confirm(file_name):
                choices.append(file_name)
        file_basenames = choices

    files_to_copy = [
        file for file in os.scandir(path) if os.path.basename(file) in file_basenames
    ]

    from bklg.tasks.db.clean import clean_and_write

    tasks = []
    # import IPython; IPython.embed()
    for file in files_to_copy:
        tasks.append(clean_and_write.s(file.path, postfix))
    results = group(tasks).apply_async().get()
    click.echo("Finished cleaning files.")

    # _csv = CleanCsv(os.path.abspath(file))
    # _csv.replace_nul()
    # click.echo(f"Copying {os.path.abspath(file)} to table {_csv.table}")
    # conn_string = app.config['SQLALCHEMY_DATABASE_URI'].replace("+psycopg", "")
    # conn = psycopg.connect(conn_string)
    # _csv.copy_to_table(conn)
    # _csv.del_no_nul()
    # click.echo(
    #     f"Copied {_csv.row_count-_csv.empty_pk} of {_csv.row_count} rows to {_csv.table}"
    # )
    # click.echo(f"There were {_csv.empty_pk} rows with no primary keys")
