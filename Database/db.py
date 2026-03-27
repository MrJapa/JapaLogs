"""
Database utilities for SQL Server access and upsert workflows.

This module wraps low-level `pyodbc` operations used by the API integrations.
"""

import os
import dotenv
import pyodbc
import datetime
import uuid

dotenv.load_dotenv()

class SQL:
    """
    SQL Server helper for connection management, logging, and upsert patterns.

    Attributes:
        connection (pyodbc.Connection): Open connection to the configured SQL Server database.
    """

    def __init__(self):
        """
        Initialize the SQL helper and open a database connection.

        Raises:
            Exception: If connection setup fails.
        """
        try:
            self.connection = self.connect_to_database()
        except Exception as e:
            raise

    def connect_to_database(self):
        """
        Create and return a SQL Server connection using environment variables.

        Returns:
            pyodbc.Connection: Active SQL Server connection.
        """
        server = os.getenv('SQL_SERVER')
        database = os.getenv('SQL_DATABASE')
        username = os.getenv('SQL_USERNAME')
        password = os.getenv('SQL_PASSWORD')
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str)

    def close(self):
        """Close the active database connection when present."""
        connection = getattr(self, "connection", None)
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
            finally:
                self.connection = None

    def __enter__(self):
        """Allow the SQL helper to be used as a context manager."""
        return self

    def __exit__(self, exc_type, exc, tb):
        """Close the connection when leaving a context manager scope."""
        self.close()

    def execute(self, sql: str, params: tuple | list | None = None, commit: bool = False):
        """
        Execute a SQL statement with optional commit.

        Args:
            sql (str): SQL statement to execute.
            params (tuple | list | None): Parameter values for the statement.
            commit (bool): Whether to commit the transaction after execution.

        Returns:
            None
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or ())
            if commit:
                self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def fetch_one(self, sql: str, params: tuple | list | None = None):
        """
        Execute a query and return the first row.

        Args:
            sql (str): SQL query to execute.
            params (tuple | list | None): Parameter values for the query.

        Returns:
            Any: First row from the result set, or None when empty.
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or ())
            return cursor.fetchone()
        finally:
            cursor.close()

    def fetch_all(self, sql: str, params: tuple | list | None = None):
        """
        Execute a query and return all rows.

        Args:
            sql (str): SQL query to execute.
            params (tuple | list | None): Parameter values for the query.

        Returns:
            list: All rows from the result set.
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def log(self, type, transaktion, code, timestamp=None):
        """
        Insert a transaction record and return the inserted id.

        Args:
            type (str): Transaction type/category.
            transaktion (str): Transaction identifier or correlation value.
            timestamp (datetime, optional): Timestamp override. Uses current time when omitted.

        Returns:
            int: Inserted row id from `api.Transaktion`.
        """
        if timestamp is None:
            timestamp = datetime.datetime.now()
        cursor = self.connection.cursor()
        sql = """
        INSERT INTO api.Transaktion ([Type], [Transaktion], [Timestamp], [Code])
        OUTPUT INSERTED.Id
        VALUES (?, ?, ?, ?)
        """
        cursor.execute(sql, (type, transaktion, timestamp, code))
        inserted_id = cursor.fetchone()[0]
        self.connection.commit()
        cursor.close()
        return inserted_id


    def upsert(self, table: str, data: dict, pk: str = "Id", trans_col: str = None, trans_id: int = None, trans_name: str = "UPSERT"):
        """
        Upsert a single record with optional transaction linkage.

        Args:
            table (str): Target table in `schema.Table` format.
            data (dict): Row payload to merge.
            pk (str): Primary key column name.
            trans_col (str, optional): Column that should receive a transaction id.
            trans_id (int, optional): Existing transaction id. A new one is created when omitted.
            trans_name (str): Transaction type used when creating a new log row.

        Returns:
            int: Transaction id used for the upsert operation.

        Raises:
            ValueError: If the primary key is missing from `data`.
            Exception: If the merge statement fails.
        """
        
        # Handle both single pk and composite pk
        pk_cols = pk if isinstance(pk, list) else [pk]
        
        for pk_col in pk_cols:
            if pk_col not in data:
                raise ValueError(f"Primary key '{pk_col}' not found in data.")
        
        cursor = self.connection.cursor()

        if trans_id is None:
            pk_str = data[pk_cols[0]] if len(pk_cols) == 1 else str({col: data[col] for col in pk_cols})
            trans_id = self.log(trans_name, f"{pk_str}")

        if trans_col:
            data[trans_col] = trans_id

        columns = list(data.keys())
        col_list = ", ".join([f"[{col}]" for col in columns])
        placeholders = ", ".join(["?"] * len(columns))

        update_columns = [col for col in columns if col not in pk_cols]
        update_list = ", ".join([f"[{col}]=?" for col in update_columns])
        
        # Build ON condition for composite keys
        on_condition = " AND ".join([f"target.[{col}] = incoming.[{col}]" for col in pk_cols])

        # Build WHEN MATCHED clause only if there are non-PK columns to update
        when_matched = ""
        if update_columns:
            when_matched = f"WHEN MATCHED THEN UPDATE SET {update_list}\n"

        merge_sql = f"""MERGE {table} AS target
        USING (SELECT {placeholders}) AS incoming ({col_list})
        ON {on_condition}
        {when_matched}WHEN NOT MATCHED THEN
        INSERT ({col_list}) VALUES ({placeholders});"""

        using_values = list(data.values())
        update_values = [data[col] for col in update_columns]
        insert_values = list(data.values())

        params = using_values + update_values + insert_values

        try:
            cursor.execute(merge_sql, params)
            self.connection.commit()
        except Exception as ex:
            self.connection.rollback()
            self.log("UPSERT_ERROR", f"ERROR")
            raise
        finally:
            cursor.close()

        return trans_id
    
    def batch_upsert(self, table: str, rows: list[dict], pk: str,
                     trans_col: str = None, trans_id: int = None,
                     return_ids: bool = False):
        """
        Upsert multiple rows with optional id return and transaction linkage.

        Args:
            table (str): Target table in `schema.Table` format.
            rows (list[dict]): Rows to insert or merge.
            pk (str): Primary key column or columns used for matching.
            trans_col (str, optional): Column to populate with `trans_id`.
            trans_id (int, optional): Transaction id to stamp onto rows.
            return_ids (bool): Whether inserted/updated ids should be returned.

        Returns:
            list: Inserted or matched ids when `return_ids=True`; otherwise an empty list.

        Raises:
            Exception: If insert/merge execution fails.
        """
        if not rows:
            return []
    
        clean_rows = []
        seen_pks = set()
        clean_rows = []
        seen_pks = set()
        for row in rows:
            if isinstance(pk, list):
                pk_values = tuple(row.get(p) for p in pk)
                pk_str = str(pk_values).strip()
            else:
                pk_value = row.get(pk)
                pk_str = str(pk_value).strip()
                if not pk_value or pk_str == "":
                    continue
            if pk_str in seen_pks:
                continue
            seen_pks.add(pk_str)
            clean_rows.append(row)
        
        rows = clean_rows
        
        # If all rows were filtered out (empty CVR), return early
        if not rows:
            return []

        for row in rows:
            if "customFieldsData" in row:
                val = row["customFieldsData"]
                if isinstance(val, str) and len(val) > 500:
                    row["customFieldsData"] = ""
            
            # Truncate all string values to reasonable limits to prevent SQL truncation errors
            for key, value in row.items():
                if isinstance(value, str):
                    # For most columns (nvarchar(255)), truncate to 250 chars to be safe
                    if len(value) > 250:
                        row[key] = value[:250]


        cursor = self.connection.cursor()
    
        # Gather columns from first row
        columns = list(rows[0].keys())
    
        # Inject transaction ID if requested
        if trans_col and trans_id is not None:
            for r in rows:
                r[trans_col] = trans_id
            if trans_col not in columns:
                columns.append(trans_col)
    
        # If PK is not present in rows -> INSERT ONLY with OUTPUT (identity capture)
        pk_cols = pk if isinstance(pk, list) else [pk]
        if not all(p in columns for p in pk_cols):
            col_list = ", ".join(f"[{c}]" for c in columns)
            placeholders = ", ".join("?" for _ in columns)
            values = [tuple(r.get(c) for c in columns) for r in rows]
            try:
                if return_ids:
                    # Prefer returning the identity column 'Id' when available, otherwise fall back
                    # to the first provided PK column. Some tables use identity PKs.
                    pk_first = pk if isinstance(pk, str) else (pk[0] if isinstance(pk, list) and pk else 'Id')
                    ids = []
                    # Try to OUTPUT inserted.Id first
                    try:
                        sql_id = f"INSERT INTO {table} ({col_list}) OUTPUT inserted.[Id] VALUES ({placeholders})"
                        for v in values:
                            cursor.execute(sql_id, v)
                            ids.append(cursor.fetchone()[0])
                        self.connection.commit()
                        return ids
                    except Exception:
                        # Fallback to outputting the first PK column
                        sql = f"INSERT INTO {table} ({col_list}) OUTPUT inserted.[{pk_first}] VALUES ({placeholders})"
                        ids = []
                        for v in values:
                            cursor.execute(sql, v)
                            ids.append(cursor.fetchone()[0])
                        self.connection.commit()
                        return ids
                else:
                    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
                    cursor.fast_executemany = True
                    cursor.executemany(sql, values)
                    self.connection.commit()
                    return []
            except Exception as ex:
                self.connection.rollback()
                raise
            finally:
                cursor.close()
    
        # Otherwise -> MERGE upsert using a UNIQUE local temp table
        tmp = f"#tmp_{uuid.uuid4().hex}"          # unique per invocation
        col_list = ", ".join(f"[{c}]" for c in columns)
        create_temp = f"CREATE TABLE {tmp} ({', '.join([f'[{col}] NVARCHAR(MAX)' for col in columns])});"
        drop_if_exists = f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp};"
    
        try:
            # Ensure clean temp table
            cursor.execute(drop_if_exists)
            cursor.execute(create_temp)
    
            # Load data into temp
            values = [tuple(r.get(col) for col in columns) for r in rows]
            placeholders = ", ".join([f"CAST(? AS NVARCHAR(MAX))" for _ in columns])
            cursor.fast_executemany = True
            cursor.executemany(f"INSERT INTO {tmp} ({col_list}) VALUES ({placeholders})", values)
    
            pk_cols = pk if isinstance(pk, list) else [pk]
            update_cols = [col for col in columns if col not in pk_cols]
            update_list = ", ".join([f"target.[{col}] = source.[{col}]" for col in update_cols])
            source_cols = ", ".join([f"source.[{col}]" for col in columns])
            on_clause = " AND ".join([f"target.[{col}] = source.[{col}]" for col in pk_cols])
    
            
            # Build WHEN MATCHED clause
            when_matched = ""
            if update_cols:
                when_matched = f"WHEN MATCHED THEN UPDATE SET {update_list}\n"
            elif return_ids:
            # Skip updating identity column
                when_matched = ""  # no columns to update


            # Build MERGE without inline OUTPUT; if return_ids is requested we'll append a single OUTPUT clause
            merge_sql_body = f"""MERGE {table} AS target
            USING (SELECT * FROM {tmp}) AS source
            ON {on_clause}
            {when_matched}WHEN NOT MATCHED THEN
            INSERT ({col_list}) VALUES ({source_cols})"""

            if return_ids:
                merge_sql = merge_sql_body + " OUTPUT inserted.id;"
            else:
                merge_sql = merge_sql_body + ";"

            cursor.execute(merge_sql)
            # If caller requested IDs, fetch them by querying the target table per input row.
            ids = []
            if return_ids:
                # Prefer selecting the identity Id column from target; fall back to first PK column.
                pk_first = pk if isinstance(pk, str) else (pk[0] if isinstance(pk, list) and pk else 'Id')
                for r in rows:
                    where_expr = " AND ".join([f"[{col}] = ?" for col in pk_cols])
                    params_where = tuple(r.get(col) for col in pk_cols)
                    # First try to get identity Id
                    try:
                        select_sql = f"SELECT [Id] FROM {table} WHERE {where_expr};"
                        cursor.execute(select_sql, params_where)
                        fetched = cursor.fetchone()
                        ids.append(fetched[0] if fetched else None)
                    except Exception:
                        try:
                            select_sql = f"SELECT [{pk_first}] FROM {table} WHERE {where_expr};"
                            cursor.execute(select_sql, params_where)
                            fetched = cursor.fetchone()
                            ids.append(fetched[0] if fetched else None)
                        except Exception:
                            ids.append(None)
            self.connection.commit()
            return ids
        except Exception as ex:
            self.connection.rollback()
            raise
        finally:
            try:
                # Drop temp table on the way out
                cursor.execute(drop_if_exists)
            except Exception:
                pass
            cursor.close()
    
    

    def get_data(self, table: str, selects: list = None, where_clause: str = None, batch_size: int = 50, *args, **kwargs):
        """
        Stream table rows in pages using OFFSET/FETCH pagination.

        Args:
            table (str): Source table in `schema.Table` format.
            selects (list, optional): Selected columns. Defaults to all columns.
            where_clause (str, optional): Additional SQL filter expression.
            batch_size (int): Number of rows per page.
            *args: Unused compatibility arguments.
            **kwargs: Unused compatibility keyword arguments.

        Yields:
            list: A batch of rows from the database cursor.
        """
        cursor = self.connection.cursor()
        offset = 0

        if not selects:
            selects = ["*"]

        clean_selects = [col for col in selects if col and col.strip()]
        selects_str = ", ".join(clean_selects)

        null_filters = [f"{col} IS NOT NULL AND {col} <> ''" for col in clean_selects]

        where_parts = []
        if where_clause:                  # fx: "FiscalYearEnd = '2024-12-31'"
            where_parts.append(where_clause)

        if null_filters:
            where_parts.extend(null_filters)

        where_full = ""
        if where_parts:
            where_full = "WHERE " + " AND ".join(where_parts)

        while True:
            # Use the first selected column for ORDER BY to satisfy OFFSET...FETCH requirements
            order_col = clean_selects[0] if clean_selects else "1"
            sql = (
                f"SELECT {selects_str} FROM {table} "
                f"{where_full} "
                f"ORDER BY {order_col} "
                f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;"
            )

            cursor.execute(sql, (offset, batch_size))
            rows = cursor.fetchall()
            
            if not rows:
                break

            yield rows
            offset += batch_size
        cursor.close()
        
