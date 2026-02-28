import os
import logging
import psycopg2
import shutil
from pathlib import Path
from psycopg2 import pool, sql, Error
from psycopg2.extensions import quote_ident
from psycopg2.extras import RealDictCursor
from opensite.logging.base import LoggingBase
from dotenv import load_dotenv

if not Path('.env').exists(): 
    if Path('.env-template').exists():
        print("Default .env file not found, creating it from template")
        shutil.copy('.env-template', '.env')

load_dotenv()

class PostGISBase:
    def __init__(self, log_level=logging.INFO, use_pool=True):
        self.log = LoggingBase("PostGISBase", log_level)
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.database = os.getenv("POSTGRES_DB", "opensite")
        self.user = os.getenv("POSTGRES_USER", "opensite")
        self.password = os.getenv("POSTGRES_PASSWORD", "")
        
        self.conn = None
        self.pool = None

        try:
            if use_pool:
                self.pool = psycopg2.pool.SimpleConnectionPool(
                    1, 10,
                    host=self.host, database=self.database,
                    user=self.user, password=self.password
                )
            else:
                # Direct connection for heavy-duty stability
                self.conn = psycopg2.connect(
                    host=self.host, database=self.database,
                    user=self.user, password=self.password
                )
            self.log.debug(f"Connected to database: {self.database}")
        except Exception as e:
            self.log.error(f"Error connecting to Postgres: {e}")

    def get_connection(self):
        """Gets connection"""
        if self.conn: return self.conn
        else: return self.pool.getconn()

    def return_connection(self, conn):
        """Returns connection to pool"""
        if self.pool: self.pool.putconn(conn)

    def close_connection(self):
        """Closes postgis connection and clears any associated memory"""
        if self.pool:
            self.log.debug("Closing all connections in the pool to release memory")
            self.pool.closeall()
        if self.conn:
            self.log.debug("Closing main database connection (no pool)")
            self.conn.close()

    def cancel_own_queries(self):
        """
        Cancels all active queries belonging to the current connection user.
        """
        # pid <> pg_backend_pid() ensures the app doesn't commit suicide 
        # by killing the connection it is using to send the cancel signal.
        cancel_all_queries = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = current_user AND state = 'active' AND pid <> pg_backend_pid();"

        return self.execute_query(cancel_all_queries)

    def drop_table(self, table_name, schema='public', cascade=True):
        """
        Drops a table from the database safely.
        
        Args:
            table_name (str): The name of the table to drop.
            schema (str): The schema where the table resides.
            cascade (bool): If True, automatically drops objects that depend 
                            on the table (like indexes, views, and sequences).
        """
        # Use sql.Identifier to safely handle table names and schemas
        # This prevents SQL injection and handles case-sensitivity
        drop_stmt = "DROP TABLE IF EXISTS {schema}.{table}"
        if cascade:
            drop_stmt += " CASCADE"

        query = sql.SQL(drop_stmt).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table_name)
        )

        conn = self.get_connection()
        try:
            # Reset transaction state in case of previous errors in the pool
            conn.rollback() 
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                if hasattr(self, 'log'):
                    self.log.debug(f"Successfully dropped table: {table_name}")
                return True
        except Error as e:
            conn.rollback()
            if hasattr(self, 'log'):
                self.log.error(f"Failed to drop table {table_name}: {e}")
            return False
        finally:
            self.return_connection(conn)

    def copy_table(self, source_table, dest_table):
        """
        Copies source_table to dest_table including all indexes using 
        composable SQL identifiers for safety.
        """
        # Define parameters using identifiers for table names
        dbparams = {
            "source": sql.Identifier(source_table),
            "dest": sql.Identifier(dest_table)
        }

        # Use INCLUDING ALL to ensure GIST indexes and constraints are copied
        query = sql.SQL("""
            DROP TABLE IF EXISTS {dest};
            CREATE TABLE {dest} (LIKE {source} INCLUDING ALL);
            INSERT INTO {dest} SELECT * FROM {source};
            ANALYZE {dest};
        """).format(**dbparams)

        try:
            self.execute_query(query)
            self.log.info(f"Successfully deep-copied {source_table} to {dest_table}")
        except Exception as e:
            self.log.error(f"Failed to copy table via PostGIS: {e}")
            raise

    def execute_query(self, query, params=None, autocommit=False):
        """
        Standard wrapper to execute a command.
        If autocommit is True, it runs outside a transaction block (required for VACUUM).
        """
        conn = self.get_connection()
        try:
            # Switch mode based on the request
            conn.autocommit = autocommit
            
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                
                # We only manually commit if we are NOT in autocommit mode
                if not autocommit:
                    conn.commit()
        except Exception as e:
            if not autocommit:
                conn.rollback()
            raise e
        finally:
            # Reset to default and return to pool
            conn.autocommit = False
            self.return_connection(conn)
            
    def fetch_all(self, query, params=None):
        """Standard wrapper to fetch results as a list of dictionaries."""

        conn = self.get_connection()
        try:
            # Specifying RealDictCursor makes fetchall() return [ {'col': val}, ... ]
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        finally:
            self.return_connection(conn)

    def get_table_names(self, schema='public'):
        """
        Returns a set of physical table names present in the specified schema.
        Uses the standard fetch_all tuple-return format.
        """
        sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
        """
        results = self.fetch_all(sql, (schema,))
        
        return {row['table_name'] for row in results}
    
    def table_exists(self, table_name, schema='public'):
        """
        Checks if specific table exists in the PostGIS database.
        
        Uses pg_class to correctly handle tables with leading underscores
        or case-sensitive names that were created using sql.Identifier
        """
        # We query the system catalog. 
        # relkind = 'r' ensures we are looking for a standard Table.
        query = sql.SQL("""
        SELECT EXISTS (
            SELECT 1 
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = {schema_lit}
              AND c.relname = {table_lit}
              AND c.relkind = 'r'
        );
        """).format(schema_lit = sql.Literal(schema), table_lit  = sql.Literal(table_name))

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result is not None:
                    if isinstance(result, (tuple, list)):
                        return bool(result[0])
                    return bool(result)
                return False
        except Error as e:
            self.log.error(f"Database error checking table existence for {table_name}: {e}")
            return False
        finally:
            self.return_connection(conn)

    def get_ogr_connection_string(self):
        """
        Returns the connection string formatted specifically for GDAL/OGR tools.
        Wraps values in single quotes to handle special characters safely.
        """
        return f"PG:host='{self.host}' dbname='{self.database}' user='{self.user}' password='{self.password}'"
    
    def add_table_comment(self, table_id, comment):
        """
        Adds a comment to a specific table using the existing execute_query logic.
        """
        # quote_ident handles the table name: _opensite_table -> "_opensite_table"
        # The %s in the query handles the comment string properly.
        conn = self.get_connection()
        try:
            safe_table = quote_ident(table_id, conn)
            sql = f"COMMENT ON TABLE {safe_table} IS %s"
            
            self.execute_query(sql, (comment,))
            self.log.debug(f"Comment added to {table_id}")
            return True
        except Exception as e:
            self.log.error(f"Failed to add comment to {table_id}: {e}")
            return False
        finally:
            self.return_connection(conn)

    def extract_crs_as_number(self, crs):
        """
        Extracts CRS as integer
        """

        return int(str(crs).replace('EPSG:', ''))
    
    def get_table_bounds(self, table_name, crs_input, crs_output):
        """
        Get bounds of all geometries in table
        """

        dbparams = {
            "crs_input": sql.Literal(self.extract_crs_as_number(crs_input)),
            "crs_output": sql.Literal(self.extract_crs_as_number(crs_output)),
            'table': sql.Identifier(table_name),
        }

        query_maxbounds = sql.SQL("""
        SELECT 
            ST_XMin(extent_output_crs) AS left,
            ST_YMin(extent_output_crs) AS bottom,
            ST_XMax(extent_output_crs) AS right,
            ST_YMax(extent_output_crs) AS top
        FROM (SELECT ST_Transform(ST_SetSRID(ST_Extent(geom), {crs_input}), {crs_output}) AS extent_output_crs FROM {table}) AS subquery
        """).format(**dbparams)

        try:
            results = self.fetch_all(query_maxbounds)
            return results[0]
        except Exception as e:
            self.log.error(f"PostGIS error: {e}")
            return None
