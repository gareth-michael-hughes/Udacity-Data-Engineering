import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops any pre-existing versions of the staging or analytical tables in Redshift RDB

    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB

   """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates the staging and analytical tables in Redshift RDB

    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB

   """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Create connection string to Redshift RDB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # Create cursor to execute SQL queries
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print('Table skeletons successfully created')
    
    # Close out connection to Redshift RDB
    conn.close()


if __name__ == "__main__":
    main()