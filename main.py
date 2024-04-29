import sys

import pymysql.cursors
import psycopg2


log_file = "out_log.txt"


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


def insert_into_postgres(data, pg_table):
    pg_host = 'localhost'
    pg_user = 'postgres'
    pg_password = 'postgres'
    pg_database = 'elab1'

    try:
        pg_conn = psycopg2.connect(host=pg_host,
                                   user=pg_user,
                                   password=pg_password,
                                   database=pg_database)

        cursor = pg_conn.cursor()

        # Get column names from PostgreSQL
        query = f'''SELECT column_name FROM information_schema.columns WHERE table_name = '{pg_table.split(".")[-1]}' AND table_schema = '{pg_table.split(".")[0]}';'''
#         query = f'''SELECT attname AS col
        # FROM   pg_attribute
        # WHERE  attrelid = '{pg_table}'::regclass
        # AND    attnum > 0
        # AND    NOT attisdropped
        # ORDER  BY attnum;'''

        # log(f"Query: {query}")

        # Execute the query
        cursor.execute(query)
        columns_result = cursor.fetchall()

        # Print the fetched columns for debugging
        # print("Fetched columns:", columns_result)

        columns = ['"' + column[0] + '"' for column in columns_result]

        # columns = list(data[0].keys())

        print(columns)

        # Generate SQL query
        sql_query = f"INSERT INTO {pg_table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

        # for row in data:
        #     try:
        #         cursor.execute(sql_query, tuple(row[column] for column in columns))
        #     except psycopg2.Error as e:
        #         pg_conn.rollback()
        #         print(f"Error executing SQL: {e}")
        #         break

        for row in data:
            try:
                log(f"Executing SQL: {sql_query}")
                # log(f"Parameters: {tuple(row[column] for column in columns)}")
                log(f"Parameters: {tuple(row.get(column, None) for column in columns)}")
                # cursor.execute(sql_query, tuple(row[column] for column in columns))
                cursor.execute(sql_query, tuple(row.get(column, None) for column in columns))
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

        # Get table names from MySQL
        cursor.execute("SHOW TABLES")
        tables_result = cursor.fetchall()
        tables = [table['Tables_in_elab1'] for table in tables_result]

        mysql_conn.close()

        for table in tables:
            log(f"Processing table: {table}")
            data = fetch_mysql_data(table)
            log(len(data))
            if data:
                pg_table = f"elab.{table}"  # Assuming schema is 'elab1' in PostgreSQL
                print(f'processing table => {table}')
                insert_into_postgres(data, pg_table)
    except pymysql.Error as e:
        log(f"Error connecting to MySQL: {e}")
    finally:
        print("Finished Migrating")


if __name__ == "__main__":
    main()
