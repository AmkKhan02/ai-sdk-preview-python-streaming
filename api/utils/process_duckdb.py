"""
DuckDB file processing utility for extracting column information and analytical queries.

This module provides functionality to process DuckDB files, extract
column information, and generate SQL queries using AI for analytical purposes.
"""

import os
import tempfile
import logging
import json
import time
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from datetime import datetime, date

try:
    import duckdb
except ImportError:
    duckdb = None
    logging.warning("DuckDB not installed. DuckDB processing will not be available.")

try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
except ImportError:
    genai = None
    logging.warning("Google Generative AI not installed. SQL generation will not be available.")


class DuckDBProcessingError(Exception):
    """Custom exception for DuckDB processing errors."""
    pass


class SQLGenerationError(Exception):
    """Custom exception for SQL generation errors."""
    
    def __init__(self, message: str, user_prompt: str = None, schema_info: dict = None):
        self.message = message
        self.user_prompt = user_prompt
        self.schema_info = schema_info
        super().__init__(message)


class QueryExecutionError(Exception):
    """Custom exception for SQL query execution errors."""
    
    def __init__(self, message: str, sql: str = None, db_path: str = None):
        self.message = message
        self.sql = sql
        self.db_path = db_path
        super().__init__(message)


class ResponseGenerationError(Exception):
    """Custom exception for response generation errors."""
    
    def __init__(self, message: str, query_results: list = None):
        self.message = message
        self.query_results = query_results
        super().__init__(message)


def extract_clean_response(response_data: Union[Dict[str, Any], str]) -> str:
    """
    Extracts clean text from internal JSON responses.

    This function isolates the user-facing 'answer' from the internal
    response structure and handles various malformed or unexpected response formats gracefully.

    Args:
        response_data: The response data, which can be a dictionary or a string.

    Returns:
        The extracted clean text answer, or a fallback string if extraction fails.
    """
    if isinstance(response_data, str):
        try:
            # If the string is JSON, parse it to extract the answer
            data = json.loads(response_data)
            if isinstance(data, dict) and 'answer' in data:
                return data['answer']
            return response_data  # Return original string if not a dict with 'answer'
        except json.JSONDecodeError:
            # Not a JSON string, return as is
            return response_data
    
    if isinstance(response_data, dict):
        # Primary extraction path for dictionary responses
        return response_data.get('answer', str(response_data))
    
    # Fallback for other data types (e.g., lists, tuples)
    return str(response_data)


def serialize_datetime_objects(obj):
    """
    Recursively convert datetime objects to ISO format strings for JSON serialization.
    
    Args:
        obj: Object that may contain datetime objects
        
    Returns:
        Object with datetime objects converted to strings
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_datetime_objects(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetime_objects(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(serialize_datetime_objects(item) for item in obj)
    else:
        return obj


def validate_duckdb_file(file_path: str) -> bool:
    """
    Validate if a file is a valid DuckDB database file.
    
    Args:
        file_path: Path to the file to validate
        
    Returns:
        True if the file is a valid DuckDB file, False otherwise
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        DuckDBProcessingError: If DuckDB is not available
    """
    if duckdb is None:
        raise DuckDBProcessingError("DuckDB is not installed")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        # Try to connect to the database
        conn = duckdb.connect(file_path, read_only=True)
        # Try to get table information
        conn.execute("SHOW TABLES")
        conn.close()
        return True
    except Exception as e:
        logging.warning(f"File validation failed for {file_path}: {str(e)}")
        return False


def get_table_info(file_path: str) -> Dict[str, Any]:
    """
    Get comprehensive table information from a DuckDB file including sample data and statistics.
    
    Args:
        file_path: Path to the DuckDB file
        
    Returns:
        Dictionary containing comprehensive table information:
        {
            'tables': ['table1', 'table2'],
            'schemas': {
                'table1': [
                    {
                        'name': 'id', 
                        'type': 'INTEGER', 
                        'null': False,
                        'sample_values': [1, 2, 3],
                        'unique_count': 100,
                        'min_value': 1,
                        'max_value': 100
                    }
                ]
            },
            'row_counts': {'table1': 1000, 'table2': 500},
            'relationships': [
                {'from_table': 'table1', 'from_column': 'id', 'to_table': 'table2', 'to_column': 'table1_id'}
            ],
            'primary_table': 'table1',
            'file_size': 1024000
        }
        
    Raises:
        DuckDBProcessingError: If processing fails
        FileNotFoundError: If file doesn't exist
    """
    if duckdb is None:
        raise DuckDBProcessingError("DuckDB is not installed")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        conn = duckdb.connect(file_path, read_only=True)
        
        # Get all tables
        tables_result = conn.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in tables_result] if tables_result else []
        
        if not tables:
            conn.close()
            raise DuckDBProcessingError("No tables found in the database")
        
        # Get comprehensive schema information for each table
        table_schemas = {}
        row_counts = {}
        
        for table_name in tables:
            try:
                # Get basic column information
                columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                
                # Get row count for this table
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_count = count_result[0] if count_result else 0
                row_counts[table_name] = row_count
                
                columns = []
                for row in columns_result:
                    column_info = {
                        'name': row[0],
                        'type': row[1],
                        'null': row[2] if len(row) > 2 else None
                    }
                    
                    # Add enhanced column statistics and sample data
                    column_info.update(_get_column_statistics(conn, table_name, row[0], row[1], row_count))
                    columns.append(column_info)
                
                table_schemas[table_name] = columns
                
            except Exception as e:
                logging.warning(f"Failed to get comprehensive schema for table {table_name}: {str(e)}")
                # Fallback to basic schema
                try:
                    columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                    columns = []
                    for row in columns_result:
                        columns.append({
                            'name': row[0],
                            'type': row[1],
                            'null': row[2] if len(row) > 2 else None,
                            'sample_values': [],
                            'unique_count': None,
                            'min_value': None,
                            'max_value': None
                        })
                    table_schemas[table_name] = columns
                    row_counts[table_name] = 0
                except Exception as fallback_e:
                    logging.error(f"Failed to get basic schema for table {table_name}: {str(fallback_e)}")
                    table_schemas[table_name] = []
                    row_counts[table_name] = 0
        
        # Detect relationships between tables
        relationships = _detect_table_relationships(conn, tables, table_schemas)
        
        # Determine primary table (largest by row count)
        primary_table = max(row_counts.items(), key=lambda x: x[1])[0] if row_counts else tables[0] if tables else None
        
        # Get file size
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        conn.close()
        
        return {
            'tables': tables,
            'schemas': table_schemas,
            'row_counts': row_counts,
            'relationships': relationships,
            'primary_table': primary_table,
            'file_size': file_size
        }
        
    except Exception as e:
        raise DuckDBProcessingError(f"Failed to get table information: {str(e)}")


def get_cols(file_path: str) -> List[str]:
    """
    Extract column names from a DuckDB file.
    
    This function extracts column names from the first table in the DuckDB file.
    If multiple tables exist, it returns columns from the first table found.
    
    Args:
        file_path: Path to the DuckDB file
        
    Returns:
        List of column names from the first table
        
    Raises:
        ValueError: If file is not a valid DuckDB file
        FileNotFoundError: If file doesn't exist
        DuckDBProcessingError: If processing fails
    """
    if duckdb is None:
        raise DuckDBProcessingError("DuckDB is not installed")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Validate the file first
    if not validate_duckdb_file(file_path):
        raise ValueError(f"Invalid DuckDB file: {file_path}")
    
    try:
        # Get table information
        table_info = get_table_info(file_path)
        
        if not table_info['tables']:
            raise DuckDBProcessingError("No tables found in the database")
        
        # Get columns from the first table
        primary_table = table_info['primary_table']
        if primary_table and primary_table in table_info['schemas']:
            columns = [col['name'] for col in table_info['schemas'][primary_table]]
            if not columns:
                raise DuckDBProcessingError(f"No columns found in table: {primary_table}")
            return columns
        else:
            raise DuckDBProcessingError("Unable to extract column information")
            
    except (ValueError, FileNotFoundError, DuckDBProcessingError):
        # Re-raise these specific exceptions
        raise
    except Exception as e:
        raise DuckDBProcessingError(f"Unexpected error during column extraction: {str(e)}")


def cleanup_temp_file(file_path: str) -> None:
    """
    Clean up temporary files safely.
    
    Args:
        file_path: Path to the file to clean up
    """
    try:
        if os.path.exists(file_path):
            # Ensure we're only cleaning up files in temp directories
            path_obj = Path(file_path)
            temp_dir = Path(tempfile.gettempdir())
            
            # Check if the file is in a temp directory for safety
            if temp_dir in path_obj.parents or path_obj.parent == temp_dir:
                os.remove(file_path)
                logging.info(f"Cleaned up temporary file: {file_path}")
            else:
                logging.warning(f"Skipped cleanup of non-temp file: {file_path}")
    except Exception as e:
        logging.error(f"Failed to cleanup file {file_path}: {str(e)}")


def get_sql_queries(user_prompt: str, columns_info: dict) -> List[str]:
    """
    Generate SQL queries from natural language using Gemini API.
    
    Args:
        user_prompt: User's natural language question
        columns_info: Database schema information from get_table_info()
        
    Returns:
        List of SQL query strings
        
    Raises:
        SQLGenerationError: If query generation fails
    """
    if genai is None:
        raise SQLGenerationError(
            "Google Generative AI is not installed",
            user_prompt=user_prompt,
            schema_info=columns_info
        )
    
    # Get API key from environment
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise SQLGenerationError(
            "GEMINI_API_KEY environment variable not set",
            user_prompt=user_prompt,
            schema_info=columns_info
        )
    
    try:
        # Configure the API
        genai.configure(api_key=api_key)
        
        # Create the model
        model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Format schema information for the prompt
        schema_text = _format_schema_for_prompt(columns_info)
        
        # Create structured prompt for SQL generation
        prompt = f"""You are a SQL expert. Generate SQL queries to answer the user's question based on the provided database schema.

Database Schema:
{schema_text}

User Question: {user_prompt}

Requirements:
1. Generate only valid SQL queries that can run against DuckDB
2. Use appropriate aggregations, filters, and joins based on the available tables and columns
3. Consider data types and constraints shown in the schema
4. Return multiple queries if needed to fully answer the question
5. Use proper SQL formatting and best practices
6. Only reference tables and columns that exist in the provided schema
7. Use standard SQL syntax compatible with DuckDB
8. IMPORTANT DuckDB-specific function names:
   - Use 'julian' function instead of 'JULIANDAY' for Julian day calculations
   - Use 'DATE_DIFF' or 'DATEDIFF' for date differences (not DATE_SUB)
   - Use 'DATE_PART' or 'EXTRACT' for extracting date parts
   - Use 'STRPTIME' instead of 'STR_TO_DATE' for parsing dates
   - Refer to DuckDB documentation for other function names if unsure

SPECIAL INSTRUCTIONS FOR COMPREHENSIVE ANALYSIS:
- If the question asks about "most leads", "highest volume", or similar superlatives, generate queries that show ALL time periods (months/quarters/years) with their counts, not just the maximum
- For temporal questions, always include comprehensive breakdowns (e.g., all months, not just the peak month)
- For comparison questions, provide complete datasets that allow full comparison across all relevant dimensions
- Use ORDER BY and LIMIT appropriately, but ensure the query provides enough context for complete analysis

Return your response as a JSON array of SQL query strings. Example format:
["SELECT * FROM table1 WHERE condition", "SELECT COUNT(*) FROM table2"]

Only return the JSON array, no additional text or explanation."""

        # Generate content with safety settings
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        
        response = model.generate_content(
            prompt,
            safety_settings=safety_settings,
            generation_config=genai.types.GenerationConfig(
                temperature=0.0,  # Set to 0 for completely deterministic SQL generation
                max_output_tokens=2048,
            )
        )
        
        if not response.text:
            raise SQLGenerationError(
                "Empty response from Gemini API",
                user_prompt=user_prompt,
                schema_info=columns_info
            )
        
        # Parse and validate the JSON response
        sql_queries = _parse_and_validate_sql_response(response.text, user_prompt, columns_info)
        
        if not sql_queries:
            raise SQLGenerationError(
                "No valid SQL queries generated",
                user_prompt=user_prompt,
                schema_info=columns_info
            )
        
        logging.info(f"Generated {len(sql_queries)} SQL queries for prompt: {user_prompt[:50]}...")
        return sql_queries
        
    except json.JSONDecodeError as e:
        raise SQLGenerationError(
            f"Failed to parse JSON response from Gemini API: {str(e)}",
            user_prompt=user_prompt,
            schema_info=columns_info
        )
    except Exception as e:
        if isinstance(e, SQLGenerationError):
            raise
        raise SQLGenerationError(
            f"Unexpected error during SQL generation: {str(e)}",
            user_prompt=user_prompt,
            schema_info=columns_info
        )


def execute_sql_queries(sql_queries: List[str], db_path: str, result_format: str = "fetchall") -> List[Dict[str, Any]]:
    """
    Execute SQL queries against DuckDB database.
    
    Args:
        sql_queries: List of SQL query strings to execute
        db_path: Path to the DuckDB database file
        result_format: Format for results - "fetchall" for list of tuples, "df" for DataFrame
        
    Returns:
        List of dictionaries containing query results and metadata:
        [
            {
                "sql": "SELECT * FROM table1",
                "success": True,
                "data": [...],  # Query results
                "columns": ["col1", "col2"],  # Column names
                "row_count": 100,
                "execution_time_ms": 45.2,
                "error": None
            },
            ...
        ]
        
    Raises:
        QueryExecutionError: If query execution fails
        FileNotFoundError: If database file doesn't exist
        DuckDBProcessingError: If DuckDB is not available
    """
    if duckdb is None:
        raise DuckDBProcessingError("DuckDB is not installed")
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    if not sql_queries:
        raise QueryExecutionError("No SQL queries provided", db_path=db_path)
    
    if result_format not in ["fetchall", "df"]:
        raise QueryExecutionError(f"Invalid result format: {result_format}. Must be 'fetchall' or 'df'", db_path=db_path)
    
    # Validate database file
    if not validate_duckdb_file(db_path):
        raise QueryExecutionError(f"Invalid DuckDB file: {db_path}", db_path=db_path)
    
    results = []
    conn = None
    
    try:
        # Establish database connection
        conn = duckdb.connect(db_path, read_only=True)
        logging.info(f"Connected to DuckDB database: {db_path}")
        
        for i, sql_query in enumerate(sql_queries):
            if not sql_query or not isinstance(sql_query, str):
                results.append({
                    "sql": str(sql_query),
                    "success": False,
                    "data": None,
                    "columns": [],
                    "row_count": 0,
                    "execution_time_ms": 0.0,
                    "error": "Invalid SQL query: empty or not a string"
                })
                continue
            
            sql_query = sql_query.strip()
            if not sql_query:
                results.append({
                    "sql": sql_query,
                    "success": False,
                    "data": None,
                    "columns": [],
                    "row_count": 0,
                    "execution_time_ms": 0.0,
                    "error": "Empty SQL query"
                })
                continue
            
            # Execute individual query with timing
            query_result = _execute_single_query(conn, sql_query, result_format, i)
            results.append(query_result)
        
        logging.info(f"Executed {len(sql_queries)} queries against database: {db_path}")
        return results
        
    except Exception as e:
        if isinstance(e, (QueryExecutionError, FileNotFoundError, DuckDBProcessingError)):
            raise
        raise QueryExecutionError(
            f"Unexpected error during query execution: {str(e)}",
            db_path=db_path
        )
    finally:
        # Clean up database connection
        if conn:
            try:
                conn.close()
                logging.debug("Database connection closed")
            except Exception as e:
                logging.warning(f"Error closing database connection: {str(e)}")


def generate_response(user_prompt: str, sql_queries: List[str], query_results: List[Dict[str, Any]]) -> str:
    """
    Generate natural language response from query results using Gemini API.
    
    Args:
        user_prompt: Original user question
        sql_queries: SQL queries that were executed
        query_results: Results from query execution
        
    Returns:
        Natural language response string
        
    Raises:
        ResponseGenerationError: If response generation fails
    """
    if genai is None:
        logging.warning("Google Generative AI not available, using fallback response generation")
        return _generate_fallback_response(user_prompt, sql_queries, query_results)
    
    # Get API key from environment
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        logging.warning("GEMINI_API_KEY not set, using fallback response generation")
        return _generate_fallback_response(user_prompt, sql_queries, query_results)
    
    try:
        # Configure the API
        genai.configure(api_key=api_key)
        
        # Create the model
        model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Format query results for the prompt
        results_text = _format_query_results_for_prompt(sql_queries, query_results)
        
        # Create structured prompt for response synthesis
        prompt = f"""You are a data analyst. Create a clear, insightful response to the user's question based on the SQL query results.

Original Question: {user_prompt}

SQL Queries Executed:
{_format_sql_queries_for_prompt(sql_queries)}

Query Results:
{results_text}

Requirements:
1. Provide a direct answer to the user's question
2. Include specific numbers and insights from the data
3. Explain any trends or patterns found in the results
4. Use clear, non-technical language that anyone can understand
5. If multiple queries were executed, synthesize the results coherently
6. If no data was found, explain what this means in context
7. ONLY suggest follow-up questions if the query results are incomplete or if there are obvious gaps in the data that would require additional analysis
8. If you have comprehensive data that fully answers the question (like monthly breakdowns, comparisons, or complete datasets), provide a complete answer without suggesting to look at additional data
9. Keep the response concise but comprehensive

Generate a comprehensive but concise response that directly answers the user's question. If you have all the data needed to fully answer the question, provide a complete response without suggesting additional analysis."""

        # Generate content with safety settings
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        
        response = model.generate_content(
            prompt,
            safety_settings=safety_settings,
            generation_config=genai.types.GenerationConfig(
                temperature=0.0,  # Set to 0 for completely deterministic response generation
                max_output_tokens=1024,
            )
        )
        
        if not response.text:
            logging.warning("Empty response from Gemini API, using fallback")
            return _generate_fallback_response(user_prompt, sql_queries, query_results)
        
        # Clean up the response text
        response_text = response.text.strip()
        
        if not response_text:
            logging.warning("Empty response text after cleaning, using fallback")
            return _generate_fallback_response(user_prompt, sql_queries, query_results)
        
        logging.info(f"Generated natural language response for prompt: {user_prompt[:50]}...")
        return response_text
        
    except Exception as e:
        logging.error(f"Error during response generation: {str(e)}")
        logging.info("Falling back to template-based response generation")
        return _generate_fallback_response(user_prompt, sql_queries, query_results)


def _execute_single_query(conn: 'duckdb.DuckDBPyConnection', sql_query: str, result_format: str, query_index: int) -> Dict[str, Any]:
    """
    Execute a single SQL query and return formatted results with metadata.
    
    Args:
        conn: DuckDB connection object
        sql_query: SQL query string to execute
        result_format: Format for results - "fetchall" or "df"
        query_index: Index of the query for logging purposes
        
    Returns:
        Dictionary containing query results and metadata
    """
    start_time = time.time()
    
    try:
        # Execute the query
        logging.debug(f"Executing query {query_index}: {sql_query[:100]}...")
        
        if result_format == "df":
            # Use df() method for DataFrame results
            try:
                result = conn.execute(sql_query).df()
                if not result.empty:
                    raw_data = result.to_dict('records')
                    # Serialize datetime objects to strings for JSON compatibility
                    data = serialize_datetime_objects(raw_data)
                    columns = result.columns.tolist()
                else:
                    data = []
                    columns = []
                row_count = len(result)
            except AttributeError:
                # Fallback if df() method is not available
                logging.warning("df() method not available, falling back to fetchall()")
                cursor = conn.execute(sql_query)
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                row_count = len(data) if data else 0
                # Convert to list of dictionaries for consistency
                if data and columns:
                    raw_dict_data = [dict(zip(columns, row)) for row in data]
                    # Serialize datetime objects to strings for JSON compatibility
                    data = serialize_datetime_objects(raw_dict_data)
        else:
            # Use fetchall() method for list of tuples
            cursor = conn.execute(sql_query)
            raw_data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            row_count = len(raw_data) if raw_data else 0
            
            # Convert to list of dictionaries for consistent format
            if raw_data and columns:
                raw_dict_data = [dict(zip(columns, row)) for row in raw_data]
                # Serialize datetime objects to strings for JSON compatibility
                data = serialize_datetime_objects(raw_dict_data)
            else:
                data = []
        
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        logging.debug(f"Query {query_index} executed successfully: {row_count} rows, {execution_time:.2f}ms")
        
        return {
            "sql": sql_query,
            "success": True,
            "data": data,
            "columns": columns,
            "row_count": row_count,
            "execution_time_ms": round(execution_time, 2),
            "error": None
        }
        
    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        error_message = str(e)
        
        logging.error(f"Query {query_index} failed: {error_message}")
        
        # Categorize common error types for better user feedback
        if "no such table" in error_message.lower():
            error_message = f"Table not found in database: {error_message}"
        elif "no such column" in error_message.lower():
            error_message = f"Column not found: {error_message}"
        elif "syntax error" in error_message.lower():
            error_message = f"SQL syntax error: {error_message}"
        elif "permission" in error_message.lower():
            error_message = f"Database access error: {error_message}"
        
        return {
            "sql": sql_query,
            "success": False,
            "data": None,
            "columns": [],
            "row_count": 0,
            "execution_time_ms": round(execution_time, 2),
            "error": error_message
        }


def _format_schema_for_prompt(columns_info: dict) -> str:
    """
    Format comprehensive database schema information for use in AI prompts.
    
    Args:
        columns_info: Enhanced schema information from get_table_info()
        
    Returns:
        Formatted schema string for prompt inclusion
    """
    if not columns_info or 'tables' not in columns_info or 'schemas' not in columns_info:
        return "No schema information available"
    
    schema_lines = []
    row_counts = columns_info.get('row_counts', {})
    relationships = columns_info.get('relationships', [])
    primary_table = columns_info.get('primary_table')
    
    # Add database overview
    total_tables = len(columns_info['tables'])
    total_rows = sum(row_counts.values())
    schema_lines.append(f"Database Overview:")
    schema_lines.append(f"  - {total_tables} table{'s' if total_tables != 1 else ''}")
    schema_lines.append(f"  - {total_rows:,} total rows across all tables")
    if primary_table:
        schema_lines.append(f"  - Primary table: {primary_table}")
    schema_lines.append("")
    
    # Add table details
    for table_name in columns_info['tables']:
        row_count = row_counts.get(table_name, 0)
        is_primary = table_name == primary_table
        primary_indicator = " (PRIMARY)" if is_primary else ""
        
        schema_lines.append(f"Table: {table_name}{primary_indicator}")
        schema_lines.append(f"  - Row count: {row_count:,}")
        
        if table_name in columns_info['schemas']:
            columns = columns_info['schemas'][table_name]
            if columns:
                schema_lines.append("  - Columns:")
                for col in columns:
                    col_name = col.get('name', 'unknown')
                    col_type = col.get('type', 'unknown')
                    nullable = col.get('null', None)
                    unique_count = col.get('unique_count')
                    sample_values = col.get('sample_values', [])
                    min_value = col.get('min_value')
                    max_value = col.get('max_value')
                    
                    # Basic column info
                    null_info = " (nullable)" if nullable else " (not null)" if nullable is False else ""
                    col_line = f"    * {col_name}: {col_type}{null_info}"
                    
                    # Add statistics if available
                    stats_parts = []
                    if unique_count is not None:
                        uniqueness = "unique" if unique_count == row_count else f"{unique_count:,} distinct values"
                        stats_parts.append(uniqueness)
                    
                    if min_value is not None and max_value is not None:
                        if min_value == max_value:
                            stats_parts.append(f"constant value: {min_value}")
                        else:
                            stats_parts.append(f"range: {min_value} to {max_value}")
                    
                    if sample_values:
                        sample_str = ", ".join(str(v) for v in sample_values[:3])
                        if len(sample_values) > 3:
                            sample_str += "..."
                        stats_parts.append(f"examples: {sample_str}")
                    
                    if stats_parts:
                        col_line += f" [{'; '.join(stats_parts)}]"
                    
                    schema_lines.append(col_line)
            else:
                schema_lines.append("    No column information available")
        else:
            schema_lines.append("    No schema information available")
        
        schema_lines.append("")
    
    # Add relationship information
    if relationships:
        schema_lines.append("Table Relationships:")
        for rel in relationships:
            if rel.get('relationship_type') == 'junction_table':
                schema_lines.append(f"  - {rel['from_table']}: Junction table connecting multiple entities")
            else:
                schema_lines.append(f"  - {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
        schema_lines.append("")
    
    if not schema_lines:
        return "No tables found in database"
    
    return "\n".join(schema_lines)


def _parse_and_validate_sql_response(response_text: str, user_prompt: str, schema_info: dict) -> List[str]:
    """
    Parse and validate SQL queries from Gemini API response.
    
    Args:
        response_text: Raw response text from Gemini API
        user_prompt: Original user prompt for error context
        schema_info: Schema information for validation context
        
    Returns:
        List of validated SQL query strings
        
    Raises:
        SQLGenerationError: If parsing or validation fails
    """
    try:
        # Clean up the response text - remove any markdown formatting or extra text
        cleaned_text = response_text.strip()
        
        # Look for JSON array in the response
        start_idx = cleaned_text.find('[')
        end_idx = cleaned_text.rfind(']')
        
        if start_idx == -1 or end_idx == -1:
            raise SQLGenerationError(
                "No JSON array found in response",
                user_prompt=user_prompt,
                schema_info=schema_info
            )
        
        json_text = cleaned_text[start_idx:end_idx + 1]
        
        # Parse JSON
        sql_queries = json.loads(json_text)
        
        if not isinstance(sql_queries, list):
            raise SQLGenerationError(
                "Response is not a JSON array",
                user_prompt=user_prompt,
                schema_info=schema_info
            )
        
        # Validate each query
        validated_queries = []
        for i, query in enumerate(sql_queries):
            if not isinstance(query, str):
                logging.warning(f"Query {i} is not a string, skipping: {query}")
                continue
            
            query = query.strip()
            if not query:
                logging.warning(f"Query {i} is empty, skipping")
                continue
            
            # Basic SQL validation - check if it looks like a SQL query
            if not _is_valid_sql_query(query):
                logging.warning(f"Query {i} doesn't appear to be valid SQL, skipping: {query[:100]}...")
                continue
            
            validated_queries.append(query)
        
        if not validated_queries:
            raise SQLGenerationError(
                "No valid SQL queries found in response",
                user_prompt=user_prompt,
                schema_info=schema_info
            )
        
        return validated_queries
        
    except json.JSONDecodeError as e:
        raise SQLGenerationError(
            f"Failed to parse JSON from response: {str(e)}",
            user_prompt=user_prompt,
            schema_info=schema_info
        )


def _is_valid_sql_query(query: str) -> bool:
    """
    Basic validation to check if a string looks like a SQL query.
    
    Args:
        query: SQL query string to validate
        
    Returns:
        True if the query appears to be valid SQL, False otherwise
    """
    if not query or not isinstance(query, str):
        return False
    
    query_upper = query.strip().upper()
    
    # Check if it starts with a valid SQL command
    valid_starts = ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'EXPLAIN']
    
    if not any(query_upper.startswith(start) for start in valid_starts):
        return False
    
    # Basic checks for SQL structure
    if 'SELECT' in query_upper and 'FROM' not in query_upper:
        # SELECT queries should generally have FROM clause (except for simple expressions)
        if not any(keyword in query_upper for keyword in ['DUAL', 'VALUES', '1', 'NOW()', 'CURRENT_']):
            return False
    
    # Check for balanced parentheses
    if query.count('(') != query.count(')'):
        return False
    
    # Check for balanced quotes (basic check)
    single_quotes = query.count("'")
    double_quotes = query.count('"')
    if single_quotes % 2 != 0 or double_quotes % 2 != 0:
        return False
    
    return True


def _format_sql_queries_for_prompt(sql_queries: List[str]) -> str:
    """
    Format SQL queries for inclusion in AI prompts.
    
    Args:
        sql_queries: List of SQL query strings
        
    Returns:
        Formatted string of SQL queries
    """
    if not sql_queries:
        return "No SQL queries executed"
    
    formatted_queries = []
    for i, query in enumerate(sql_queries, 1):
        formatted_queries.append(f"{i}. {query}")
    
    return "\n".join(formatted_queries)


def _format_query_results_for_prompt(sql_queries: List[str], query_results: List[Dict[str, Any]]) -> str:
    """
    Format query results for inclusion in AI prompts.
    
    Args:
        sql_queries: List of SQL query strings
        query_results: List of query result dictionaries
        
    Returns:
        Formatted string of query results
    """
    if not query_results:
        return "No query results available"
    
    formatted_results = []
    
    for i, result in enumerate(query_results, 1):
        query_text = sql_queries[i-1] if i-1 < len(sql_queries) else f"Query {i}"
        
        if not result.get('success', False):
            formatted_results.append(f"Query {i} ({query_text}): FAILED - {result.get('error', 'Unknown error')}")
            continue
        
        data = result.get('data', [])
        row_count = result.get('row_count', 0)
        columns = result.get('columns', [])
        
        result_summary = f"Query {i} ({query_text[:50]}{'...' if len(query_text) > 50 else ''}):"
        result_summary += f"\n  - Returned {row_count} rows"
        
        if columns:
            result_summary += f"\n  - Columns: {', '.join(columns)}"
        
        # Include sample data (first few rows) for context
        if data and row_count > 0:
            sample_size = min(3, len(data))  # Show up to 3 sample rows
            result_summary += f"\n  - Sample data (first {sample_size} rows):"
            
            for j, row in enumerate(data[:sample_size]):
                if isinstance(row, dict):
                    # Format as key-value pairs
                    row_items = []
                    for key, value in row.items():
                        # Truncate long values
                        str_value = str(value)
                        if len(str_value) > 50:
                            str_value = str_value[:47] + "..."
                        row_items.append(f"{key}: {str_value}")
                    result_summary += f"\n    Row {j+1}: {{{', '.join(row_items)}}}"
                else:
                    # Handle tuple format
                    result_summary += f"\n    Row {j+1}: {row}"
            
            # If there are many rows, provide summary statistics
            if row_count > sample_size:
                result_summary += f"\n  - ... and {row_count - sample_size} more rows"
        elif row_count == 0:
            result_summary += "\n  - No data returned"
        
        formatted_results.append(result_summary)
    
    return "\n\n".join(formatted_results)


def _get_column_statistics(conn: 'duckdb.DuckDBPyConnection', table_name: str, column_name: str, column_type: str, row_count: int) -> Dict[str, Any]:
    """
    Get comprehensive statistics for a specific column.
    
    Args:
        conn: DuckDB connection object
        table_name: Name of the table
        column_name: Name of the column
        column_type: Data type of the column
        row_count: Total number of rows in the table
        
    Returns:
        Dictionary containing column statistics
    """
    stats = {
        'sample_values': [],
        'unique_count': None,
        'min_value': None,
        'max_value': None
    }
    
    if row_count == 0:
        return stats
    
    try:
        # Get sample values (up to 5 distinct values)
        sample_query = f"""
        SELECT DISTINCT "{column_name}" 
        FROM {table_name} 
        WHERE "{column_name}" IS NOT NULL 
        LIMIT 5
        """
        sample_result = conn.execute(sample_query).fetchall()
        stats['sample_values'] = [row[0] for row in sample_result] if sample_result else []
        
        # Get unique count
        unique_query = f'SELECT COUNT(DISTINCT "{column_name}") FROM {table_name}'
        unique_result = conn.execute(unique_query).fetchone()
        stats['unique_count'] = unique_result[0] if unique_result else None
        
        # Get min/max values for numeric and date types
        if _is_numeric_or_date_type(column_type):
            try:
                minmax_query = f'SELECT MIN("{column_name}"), MAX("{column_name}") FROM {table_name} WHERE "{column_name}" IS NOT NULL'
                minmax_result = conn.execute(minmax_query).fetchone()
                if minmax_result:
                    stats['min_value'] = minmax_result[0]
                    stats['max_value'] = minmax_result[1]
            except Exception as e:
                logging.debug(f"Failed to get min/max for {table_name}.{column_name}: {str(e)}")
        
    except Exception as e:
        logging.debug(f"Failed to get statistics for {table_name}.{column_name}: {str(e)}")
    
    return stats


def _is_numeric_or_date_type(column_type: str) -> bool:
    """
    Check if a column type is numeric or date-based for min/max calculations.
    
    Args:
        column_type: The column data type
        
    Returns:
        True if the type supports min/max operations
    """
    if not column_type:
        return False
    
    column_type_upper = column_type.upper()
    numeric_types = [
        'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
        'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE', 'FLOAT',
        'DATE', 'TIME', 'TIMESTAMP', 'DATETIME'
    ]
    
    return any(num_type in column_type_upper for num_type in numeric_types)


def _detect_table_relationships(conn: 'duckdb.DuckDBPyConnection', tables: List[str], table_schemas: Dict[str, List[Dict]]) -> List[Dict[str, str]]:
    """
    Detect potential relationships between tables based on column names and foreign key patterns.
    
    Args:
        conn: DuckDB connection object
        tables: List of table names
        table_schemas: Dictionary of table schemas
        
    Returns:
        List of detected relationships
    """
    relationships = []
    
    if len(tables) < 2:
        return relationships
    
    try:
        # Look for foreign key patterns (column names that reference other tables)
        for table_name in tables:
            if table_name not in table_schemas:
                continue
                
            columns = table_schemas[table_name]
            
            for column in columns:
                column_name = column.get('name', '').lower()
                
                # Check if column name suggests a foreign key relationship
                for other_table in tables:
                    if other_table == table_name:
                        continue
                    
                    other_table_lower = other_table.lower()
                    
                    # Common foreign key patterns
                    fk_patterns = [
                        f"{other_table_lower}_id",
                        f"{other_table_lower}id",
                        f"id_{other_table_lower}",
                        f"{other_table_lower}_key"
                    ]
                    
                    # Also check for singular form (e.g., customer_id for customers table)
                    if other_table_lower.endswith('s'):
                        singular_form = other_table_lower[:-1]
                        fk_patterns.extend([
                            f"{singular_form}_id",
                            f"{singular_form}id",
                            f"id_{singular_form}",
                            f"{singular_form}_key"
                        ])
                    
                    if column_name in fk_patterns:
                        # Check if the referenced table has an 'id' column
                        other_columns = table_schemas.get(other_table, [])
                        id_columns = [col for col in other_columns if col.get('name', '').lower() in ['id', 'key', f"{other_table_lower}_id"]]
                        
                        if id_columns:
                            relationship = {
                                'from_table': table_name,
                                'from_column': column['name'],
                                'to_table': other_table,
                                'to_column': id_columns[0]['name'],
                                'relationship_type': 'foreign_key'
                            }
                            
                            # Avoid duplicate relationships
                            if not any(r['from_table'] == relationship['from_table'] and 
                                     r['from_column'] == relationship['from_column'] and
                                     r['to_table'] == relationship['to_table'] 
                                     for r in relationships):
                                relationships.append(relationship)
        
        # Look for junction tables (tables with only foreign key columns)
        for table_name in tables:
            if table_name not in table_schemas:
                continue
                
            columns = table_schemas[table_name]
            if len(columns) <= 3:  # Junction tables typically have 2-3 columns
                fk_columns = []
                for column in columns:
                    column_name = column.get('name', '').lower()
                    if any(pattern in column_name for pattern in ['_id', 'id_', '_key']) and column_name != 'id':
                        fk_columns.append(column)
                
                # If most columns are foreign keys, this might be a junction table
                if len(fk_columns) >= 2 and len(fk_columns) >= len(columns) - 1:
                    for i, fk_col1 in enumerate(fk_columns):
                        for fk_col2 in fk_columns[i+1:]:
                            relationship = {
                                'from_table': table_name,
                                'from_column': f"{fk_col1['name']}, {fk_col2['name']}",
                                'to_table': 'multiple',
                                'to_column': 'multiple',
                                'relationship_type': 'junction_table'
                            }
                            relationships.append(relationship)
                            break  # Only add one junction relationship per table
                        break
    
    except Exception as e:
        logging.debug(f"Failed to detect relationships: {str(e)}")
    
    return relationships


def _generate_fallback_response(user_prompt: str, sql_queries: List[str], query_results: List[Dict[str, Any]]) -> str:
    """
    Generate a fallback response when AI synthesis is not available.
    
    Args:
        user_prompt: Original user question
        sql_queries: SQL queries that were executed
        query_results: Results from query execution
        
    Returns:
        Formatted fallback response string
    """
    try:
        response_parts = []
        
        # Add header
        response_parts.append(f"Based on your question: \"{user_prompt}\"")
        response_parts.append("")
        
        # Check if we have any successful results
        successful_results = [r for r in query_results if r.get('success', False)]
        failed_results = [r for r in query_results if not r.get('success', False)]
        
        if not successful_results and not failed_results:
            response_parts.append("No queries were executed.")
            return "\n".join(response_parts)
        
        # Report on successful queries
        if successful_results:
            response_parts.append("Here's what I found in your data:")
            response_parts.append("")
            
            for i, result in enumerate(query_results):
                if not result.get('success', False):
                    continue
                
                query_num = i + 1
                row_count = result.get('row_count', 0)
                columns = result.get('columns', [])
                data = result.get('data', [])
                
                # Basic summary
                if row_count == 0:
                    response_parts.append(f"Query {query_num}: No matching records found.")
                elif row_count == 1:
                    response_parts.append(f"Query {query_num}: Found 1 record.")
                else:
                    response_parts.append(f"Query {query_num}: Found {row_count} records.")
                
                # Show some data if available
                if data and row_count > 0:
                    sample_size = min(3, len(data))
                    
                    if isinstance(data[0], dict) and columns:
                        # Show data in a readable format
                        for j, row in enumerate(data[:sample_size]):
                            row_items = []
                            for col in columns[:5]:  # Limit to first 5 columns
                                if col in row:
                                    value = row[col]
                                    # Format value for display
                                    if isinstance(value, (int, float)):
                                        if isinstance(value, float):
                                            formatted_value = f"{value:,.2f}" if value != int(value) else f"{int(value):,}"
                                        else:
                                            formatted_value = f"{value:,}"
                                    else:
                                        str_value = str(value)
                                        formatted_value = str_value[:30] + "..." if len(str_value) > 30 else str_value
                                    row_items.append(f"{col}: {formatted_value}")
                            
                            if row_items:
                                response_parts.append(f"  - {', '.join(row_items)}")
                    
                    if row_count > sample_size:
                        response_parts.append(f"  ... and {row_count - sample_size} more records")
                
                response_parts.append("")
        
        # Report on failed queries
        if failed_results:
            response_parts.append("Some queries encountered issues:")
            for i, result in enumerate(query_results):
                if result.get('success', False):
                    continue
                
                query_num = i + 1
                error = result.get('error', 'Unknown error')
                response_parts.append(f"Query {query_num}: {error}")
            response_parts.append("")
        
        # Add helpful closing
        if successful_results:
            response_parts.append("This data should help answer your question. Feel free to ask follow-up questions for more specific insights!")
        else:
            response_parts.append("I wasn't able to retrieve the data needed to answer your question. Please check if your database contains the expected tables and columns.")
        
        return "\n".join(response_parts)
        
    except Exception as e:
        logging.error(f"Error generating fallback response: {str(e)}")
        # Ultimate fallback
        return f"I attempted to analyze your data for the question: \"{user_prompt}\", but encountered technical difficulties. Please try rephrasing your question or check your database file."


def process_duckdb_file(file_path: str, cleanup: bool = True) -> Dict[str, Any]:
    """
    Process a DuckDB file and return comprehensive information.
    
    This is a high-level function that combines validation, column extraction,
    and optional cleanup in a single call.
    
    Args:
        file_path: Path to the DuckDB file
        cleanup: Whether to clean up the file after processing (default: True)
        
    Returns:
        Dictionary containing processing results
        
    Raises:
        DuckDBProcessingError: If processing fails
    """
    try:
        # Validate file
        if not validate_duckdb_file(file_path):
            raise DuckDBProcessingError("Invalid DuckDB file")
        
        # Get table information
        table_info = get_table_info(file_path)
        
        # Get columns from primary table
        columns = get_cols(file_path)
        
        # Get file size
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        result = {
            'columns': columns,
            'table_name': table_info['primary_table'],
            'all_tables': table_info['tables'],
            'file_size': file_size,
            'db_path': file_path,  # Include database path for analytical queries
            'status': 'success'
        }
        
        return result
        
    except Exception as e:
        error_result = {
            'columns': [],
            'table_name': None,
            'all_tables': [],
            'file_size': 0,
            'db_path': None,
            'status': 'error',
            'error': str(e)
        }
        return error_result
        
    finally:
        # Clean up if requested
        if cleanup:
            cleanup_temp_file(file_path)
