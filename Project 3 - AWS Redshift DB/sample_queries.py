import configparser
import psycopg2
from sql_queries import sample_query_list, sample_query_desc

        
def analytical_query(cur, conn):
    """Executes pre-designed sample queries on Redshift analytical tables, displaying row results and query title

    Parameters:
    cur: cursor for executing SQL queries
    conn: connection string to redshift RDB

   """
    for title in sample_query_desc:
        print(title)
        for query in sample_query_list:
            cur.execute(query)
            conn.commit()    


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Create connection string to Redshift RDB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # Create cursor to execute SQL queries
    cur = conn.cursor()

    analytical_query(cur, conn)

    # Close out connection to Redshift RDB
    conn.close()


if __name__ == "__main__":
    main()