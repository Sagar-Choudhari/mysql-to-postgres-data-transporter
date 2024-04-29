import sys

import pymysql.cursors
import psycopg2

log_file = "out_log2.txt"

# MySQL Connection Details
mysql_host_name = 'localhost'
mysql_user_name = 'root'
mysql_password_name = '123456789'
mysql_database_name = 'elab1'

# PostgreSQL Connection Details
psql_host_name = 'localhost'
psql_user_name = 'postgres'
psql_password_name = 'postgres'
psql_database_name = 'elab3'


def log(message):
    # Log the message to both console and file
    with open(log_file, "a") as f:
        print(message, file=f)  # Write to file
    print(message)


def get_primary_key_column(table_name, sql_table):
    try:
        conn = psycopg2.connect(host=psql_host_name, user=psql_user_name, password=psql_password_name, database=psql_database_name)
        cursor = conn.cursor()

        mysql_host = mysql_host_name
        mysql_user = mysql_user_name
        mysql_password = mysql_password_name
        mysql_database = mysql_database_name

        mysql_conn = pymysql.connect(host=mysql_host,
                                     user=mysql_user,
                                     password=mysql_password,
                                     database=mysql_database,
                                     cursorclass=pymysql.cursors.DictCursor)

        cursor1 = mysql_conn.cursor()
        sql_pcol_name = ''
        # Get table names from MySQL
        cursor1.execute(f"select COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_NAME='PRIMARY' AND TABLE_NAME='{sql_table}' AND TABLE_SCHEMA='{mysql_database_name}'")
        tables_result = cursor1.fetchone()
        if tables_result:
            sql_pcol_name = tables_result['COLUMN_NAME']
        mysql_conn.close()

        cursor.execute(f"""         
            SELECT a.attname FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary
        """)

        p_res1 = cursor.fetchone()
        if p_res1:
            pg_p_key_column = f'{p_res1[0]}'

            print(f"pg_p_key_column1 '{pg_p_key_column}'")
            print(f"sql_pcol_name '{sql_pcol_name}'")
            if pg_p_key_column != sql_pcol_name:
                rename_query = f"""ALTER TABLE {table_name} RENAME COLUMN {pg_p_key_column} TO "{sql_pcol_name}";"""
                print(f"rename_query: {rename_query}")
                res = cursor.execute(rename_query)
                conn.commit()
                print(f"res {res}")
                return sql_pcol_name
        else:
            pg_p_key_column = '0'

        cursor.close()
        conn.close()

        return pg_p_key_column

    except psycopg2.Error as e:
        print(f"Error retrieving primary key column name: {e}")
        return None


def fetch_mysql_data(mysql_table):
    mysql_host = mysql_host_name
    mysql_user = mysql_user_name
    mysql_password = mysql_password_name
    mysql_database = mysql_database_name

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
    pg_host = psql_host_name
    pg_user = psql_user_name
    pg_password = psql_password_name
    pg_database = psql_database_name

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

    # table = 'obbitumeniterations'

    mysql_host = mysql_host_name
    mysql_user = mysql_user_name
    mysql_password = mysql_password_name
    mysql_database = mysql_database_name

    try:

        mysql_conn = pymysql.connect(host=mysql_host,
                                     user=mysql_user,
                                     password=mysql_password,
                                     database=mysql_database,
                                     cursorclass=pymysql.cursors.DictCursor)

        cursor = mysql_conn.cursor()

        cursor.execute("SHOW TABLES")
        tables_result = cursor.fetchall()
        tables = [table['Tables_in_elab1'] for table in tables_result]

        mysql_conn.close()

        for table in tables:

            data = fetch_mysql_data(table)
            log(f"Rows in table {table}: {len(data)}")
            if data:
                pg_table = f"elab.{table}"

                p_key = get_primary_key_column(pg_table, table)
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
