from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector

def return_snowflake_cursor() -> snowflake.connector.cursor.SnowflakeCursor:
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()
