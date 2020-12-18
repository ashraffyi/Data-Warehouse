import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print("2.3 Loding Config file")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("2.4 connect to redshift database")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    #print(cur, conn)
    print("2.5 Load staging tables")
    load_staging_tables(cur, conn)
    print("2.6 Insert tables")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()