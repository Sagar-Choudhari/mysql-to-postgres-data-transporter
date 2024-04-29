import pymysql.cursors
import psycopg2

# MySQL Connection Details
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '123456789'
mysql_database = 'elab1'
mysql_table = ''

# PostgreSQL Connection Details
psql_host = 'psql_host'
psql_user = 'psql_user'
psql_password = 'psql_password'
psql_database = 'psql_database'
psql_table = 'psql_table'


def get_mysql_column_info():
    # Connect to MySQL
    mysql_conn = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = mysql_conn.cursor()

    # Get column names and count from MySQL table
    cursor.execute(f"DESCRIBE {mysql_table}")
    columns = [row['Field'] for row in cursor.fetchall()]
    column_count = cursor.rowcount

    cursor.close()
    mysql_conn.close()
    return columns, column_count


def get_psql_column_info():
    # Connect to PostgreSQL
    psql_conn = psycopg2.connect(
        host=psql_host,
        user=psql_user,
        password=psql_password,
        database=psql_database
    )
    cursor = psql_conn.cursor()

    # Get column names and count from PostgreSQL table
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{psql_table}'")
    columns = [row[0] for row in cursor.fetchall()]
    column_count = cursor.rowcount

    cursor.close()
    psql_conn.close()
    return columns, column_count


def rename_columns():
    mysql_columns, mysql_column_count = get_mysql_column_info()
    psql_columns, psql_column_count = get_psql_column_info()

    # Check if the number of columns match
    if mysql_column_count != psql_column_count:
        print("Column count mismatch between MySQL and PostgreSQL.")
        return

    # Rename columns in PostgreSQL if they don't match MySQL
    for mysql_col in mysql_columns:
        if mysql_col not in psql_columns:
            psql_conn = psycopg2.connect(
                host=psql_host,
                user=psql_user,
                password=psql_password,
                database=psql_database
            )
            cursor = psql_conn.cursor()

            cursor.execute(f"ALTER TABLE {psql_table} RENAME COLUMN {mysql_col} TO {mysql_col}")

            psql_conn.commit()
            cursor.close()
            psql_conn.close()
            print(f"Column {mysql_col} renamed in PostgreSQL.")


if __name__ == "__main__":
    rename_columns()
