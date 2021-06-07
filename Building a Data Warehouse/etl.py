import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables.
    :param cur: sql cursor
    :param conn: sql connection
    :return:
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert into tables.
    :param cur: sql cursor
    :param conn: sql connection
    :return:
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """    
    - Connects to Redshift.  
    
    - Loads data from Amazon S3 to Amazon Redshift  
    
    - Inserts data from staging tables to the fact and dimension tables
    
    - Finally, closes the connection. 
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()