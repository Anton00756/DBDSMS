import os

import psycopg2

if __name__ == '__main__':
    try:
        with psycopg2.connect(host=os.environ['GP_HOST'], port=os.environ['GP_PORT'], dbname=os.environ['GP_DB'],
                              user=os.environ['GP_USER'], password=os.environ['GP_PASSWORD']) as conn:
            with conn.cursor() as cur:
                cur.execute("select count(*) from person_and_call_time")
                # cur.execute("select * from person_and_call_time")
                print(cur.fetchall())
    except psycopg2.OperationalError:
        print('Could not connect to Greenplum')
