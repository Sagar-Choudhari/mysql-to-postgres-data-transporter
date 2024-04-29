import sys

import pymysql.cursors
import psycopg2

log_file = "out_log4.txt"


def log(message):
    # Log the message to both console and file
    with open(log_file, "a") as f:
        print(message, file=f)  # Write to file
    print(message)


def fetch_mysql_data(mysql_table):
    mysql_host = 'localhost'
    mysql_user = 'root'
    mysql_password = '123456789'
    mysql_database = 'elab1'

    try:
        mysql_conn = pymysql.connect(host=mysql_host,
                                     user=mysql_user,
                                     password=mysql_password,
                                     database=mysql_database,
                                     cursorclass=pymysql.cursors.DictCursor)

        cursor = mysql_conn.cursor()
        cursor.execute(f"SELECT * FROM {mysql_table}")
        data = cursor.fetchall()

        mysql_conn.close()

        return data
    except pymysql.Error as e:
        log(f"Error fetching data from MySQL: {e}")
        return None


def insert_into_postgres(data, pg_table, p_key):
    pg_host = 'localhost'
    pg_user = 'postgres'
    pg_password = 'postgres'
    pg_database = 'elab2'

    try:
        pg_conn = psycopg2.connect(host=pg_host,
                                   user=pg_user,
                                   password=pg_password,
                                   database=pg_database)

        cursor = pg_conn.cursor()

        query = f'''SELECT column_name FROM information_schema.columns WHERE table_name = '{pg_table.split(".")[-1]}' AND table_schema = '{pg_table.split(".")[0]}';'''

        cursor.execute(query)
        columns_result = cursor.fetchall()

        columns = ['"' + column[0] + '"' for column in columns_result]

        print(columns)

        # Generate SQL query
        sql_query = f"INSERT INTO {pg_table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

        for row in data:
            try:
                print(f"Row: {row}")
                if p_key != '0':
                    print(p_key)
                    print(f"Id: {row[p_key]} {p_key}")
                    i_id = row[p_key]
                    cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {pg_table} WHERE {'"' + p_key + '"'} = %s)",
                                   (i_id,))
                    exists = cursor.fetchone()[0]

                    if not exists:
                        log(f"Executing SQL: {sql_query}")

                        cursor.execute(sql_query, tuple(row.values()))
                        print("Inserted data!")
                    else:
                        print(
                            f"Skipping insertion of record with primary key {row[p_key]} as it already exists")

            except psycopg2.Error as e:
                pg_conn.rollback()
                log(f"Error executing SQL: {e}")
                break

        pg_conn.commit()
        pg_conn.close()

    except psycopg2.Error as e:
        log(f"Error inserting data into PostgreSQL: {e}")


def main():
    print("Started Migrating")
    sys.stdout = open(log_file, "w")

    table = 'obcementtestsettingtime'
    p_key = 'nelid'

    try:
        data = fetch_mysql_data(table)
        log(f"Rows in table {table}: {len(data)}")
        if data:
            pg_table = f"elab.{table}"
            # p_key = '"'+p_key+'"'
            print(f"Primary Key: {p_key}")
            print(f'processing table => {table}')

            insert_into_postgres(data, pg_table, p_key)

    except pymysql.Error as e:
        log(f"Error connecting to MySQL: {e}")
    finally:
        print("Finished Migrating")


if __name__ == "__main__":
    main()
