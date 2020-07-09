import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copies song and event data from S3 bucket and load into staging tables in Redshift RDB
    
    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB

   """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Extracts and transforms data from staging tables and loads to analytical tables in Redshift RDB
    
    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB

   """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Create connection string to Redshift RDB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # Create cursor to execute SQL queries
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print('Staging Tables successfully loaded ')
    insert_tables(cur, conn)
    print('Staging Tables successfully transformed into star schema')

    # Close out connection to Redshift RDB
    conn.close()


if __name__ == "__main__":
    main()