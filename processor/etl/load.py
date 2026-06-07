# etl/load.py
import os
import time
import tempfile

def upload_dataframe_to_stage(df, label, stage_name, run_time, file_format="csv"):
    from utils.connections import get_snowflake_connection

    filename = f"{label}_{run_time.strftime('%Y%m%d_%H%M%S')}.{file_format}"
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, filename)

        if file_format == "csv":
            df.to_csv(file_path, index=False, na_rep='')
        elif file_format == "json":
            df.to_json(file_path, orient="records", lines=True)
        else:
            raise ValueError("Unsupported file format for Snowflake upload.")

        print(f"Uploading {filename} to Snowflake stage {stage_name}")

        SNOWFLAKE_SCHEMA=os.getenv("SNOWFLAKE_SCHEMA")

        conn = get_snowflake_connection()
        cs = conn.cursor()
        try:
            cs.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA};")
            cs.execute(f"CREATE STAGE IF NOT EXISTS {stage_name};")
            cs.execute(f"PUT file://{file_path} @{stage_name}/ OVERWRITE = TRUE")
        finally:
            cs.close()
            conn.close()


def copy_stage_to_table(stage_name, table_name, file_format="CSV", connection=None):
    """
    Execute COPY INTO command to load staged files into a raw table.

    Args:
        stage_name: Name of Snowflake stage (e.g., "orders_stage")
        table_name: Target raw table name (e.g., "orders_raw")
        file_format: File format (CSV or JSON)
        connection: Snowflake connector connection object

    Returns:
        dict with keys: rows_copied, rows_skipped, execution_time_sec, status, error_message
    """
    # Implement COPY INTO logic
    # - Execute a COPY INTO command from @{stage_name}/ into {table_name}
    # - Handle both CSV and JSON file formats
    # - Parse the COPY INTO result to count rows copied/skipped
    # - Return a metrics dict matching the signature above
    # - Handle errors gracefully (set status="error" with error_message)
    start_time = time.time()
    cursor = connection.cursor()

    try:
        if file_format.upper() == "CSV":
            sql = f"COPY INTO {table_name} FROM @{stage_name} FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)"
        elif file_format.upper() == "JSON":
            sql = f"COPY INTO {table_name} FROM @{stage_name} FILE_FORMAT = (TYPE = 'JSON')"
        else:
            raise ValueError(f"File format {file_format} not accepted")
        
        print(f"Sending SQL query to Snowflake: {sql}")

        # send the sql to Snowflake
        cursor.execute(sql)
        # fetch sql results and put it in a var
        results = cursor.fetchall()

        rows_skipped = 0
        rows_copied = 0

        # get the third column from SQL results which is rows_loaded 
        for row in results:
            rows_copied += row[3]

        execution_time = round(time.time() - start_time, 2)

        return {
                "rows_copied": rows_copied,
                "rows_skipped": rows_skipped,
                "execution_time_sec": execution_time,
                "status": "success",
                "error_message": None
            }

    except Exception as e:
        return {
            "rows_copied": 0,
            "rows_skipped": 0,
            "execution_time_sec": round(time.time()-start_time,2),
            "status": "error",
            "error_message": str(e)
        }
    
    finally:
        cursor.close()




def clean_stage(stage_name, connection=None):
    """
    Execute REMOVE command to delete staged files after successful loading.

    Args:
        stage_name: Name of Snowflake stage to clean
        connection: Snowflake connector connection object

    Returns:
        dict with keys: files_removed, execution_time_sec, status, error_message
    """
    # Implement stage cleanup logic
    # - Execute REMOVE @{stage_name}/ to delete staged files
    # - Count how many files were removed
    # - Return a metrics dict matching the signature above
    start_time = time.time()
    cursor = connection.cursor()

    try:
        sql = f"REMOVE @{stage_name}"
        
        print(f"Sending SQL query to Snowflake: {sql}")

        # send the sql to Snowflake
        cursor.execute(sql)
        # fetch sql results and put it in a var
        results = cursor.fetchall()

        # files remove is just the length of what the SQL returns 
        files_removed = len(results)

        execution_time = round(time.time() - start_time, 2)

        return {
                "files_removed": files_removed,
                "execution_time_sec": execution_time,
                "status": "success",
                "error_message": None
            }

    except Exception as e:
        return {
            "files_removed": 0,
            "execution_time_sec": round(time.time()-start_time,2),
            "status": "error",
            "error_message": str(e)
        }
    
    finally:
        cursor.close()
