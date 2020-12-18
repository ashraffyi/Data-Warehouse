import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print("1.8 Loding Config file")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("1.9 Connecting to redshift database")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    #print(cur, conn)
    print("2.0 Droping tables")
    drop_tables(cur, conn)
    print("2.1 Create ables tables")
    create_tables(cur, conn)
    print("2.2 Closed connection to redshift database")
    conn.close()


if __name__ == "__main__":
    main()