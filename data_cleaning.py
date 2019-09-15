import sqlite3
import pandas as pd


def get_fields(table, conn):
    """
        gets all the field names from the table
    :param table: table name to get columns
    :param conn: sqlite DB connection
    :return fields: list of column names
    """

    exec_statement = 'PRAGMA table_info(' + table + ')'

    df = pd.read_sql(exec_statement, conn)
    fields = df['name'].tolist()

    return fields


def clean_up_players(testing, table):
    """
        updates all "None", "_None_", and "NA" values to NULL in the database
    :param testing: whether to use test DB or not
    :param table: table name to update
    :return:
    """
    if testing:
        conn = sqlite3.connect('TestDB.db')
    else:
        conn = sqlite3.connect('NBA_Statistics.db')

    fields = get_fields(table, conn)

    for field in fields:
        update_statement = "UPDATE " + table + " SET " + field + " = NULL WHERE " + field + \
                           " IN ('_None_', 'None', 'NA');"

        cur = conn.cursor()
        cur.execute(update_statement)
        conn.commit()
