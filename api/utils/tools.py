import requests
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import logging
import time
import os
import json
from typing import Dict, Any, Optional
from datetime import datetime, date
from .process_duckdb import get_sql_queries, execute_sql_queries, generate_response, get_table_info
from .db_session import session_manager
from .file_registry import file_registry
from .query_cache import query_cache


def ensure_json_serializable(obj):
    """
    Ensure an object is JSON serializable by converting problematic types.
    
    Args:
        obj: Object to make JSON serializable
        
    Returns:
        JSON serializable version of the object
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: ensure_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [ensure_json_serializable(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(ensure_json_serializable(item) for item in obj)
    else:
        # Test if the object is JSON serializable
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            # If not serializable, convert to string
            return str(obj)

def get_current_weather(latitude, longitude):
    # Format the URL with proper parameter substitution
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m&hourly=temperature_2m&daily=sunrise,sunset&timezone=auto"

    try:
        # Make the API call
        response = requests.get(url)

        # Raise an exception for bad status codes
        response.raise_for_status()

        # Return the JSON response
        return response.json()

    except requests.RequestException as e:
        # Handle any errors that occur during the request
        print(f"Error fetching weather data: {e}")
        return None

def create_graph(data: list, graph_type: str, title: str = "Graph", x_label: str = "X-axis", y_label: str = "Y-axis"):
    """
    Generates a graph from the provided data.
    """
    try:
        # Ensure data is in the correct format (list of dictionaries)
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            try:
                # Attempt to convert if it's a different but compatible format
                data = [dict(item) for item in data]
            except (TypeError, ValueError):
                return {"error": "Invalid data format. Expected a list of dictionaries."}

        df = pd.DataFrame(data)
        
        if df.empty:
            return {"error": "Data is empty"}

        # Set up the plot
        plt.figure(figsize=(10, 6))
        
        # Determine x and y columns
        columns = df.columns.tolist()
        
        if len(columns) < 2:
            return {"error": "Data must have at least 2 columns"}
        
        # Try to identify x and y columns intelligently
        x_col = None
        y_col = None
        
        for col in columns:
            if df[col].dtype in ['object', 'string'] or col.lower() in ['label', 'name', 'category', 'month']:
                x_col = col
            elif pd.api.types.is_numeric_dtype(df[col]) and col.lower() in ['value', 'amount', 'count', 'price']:
                y_col = col
        
        # Fallback to first two columns if not found
        if x_col is None:
            x_col = columns[0]
        if y_col is None:
            # Find first numeric column
            for col in columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    y_col = col
                    break
            if y_col is None:
                y_col = columns[1]  # Fallback to second column
        
        # Create the plot
        if graph_type == 'bar':
            plt.bar(df[x_col], df[y_col])
            plt.xticks(rotation=45, ha='right')
        elif graph_type == 'line':
            plt.plot(df[x_col], df[y_col], marker='o')
            plt.xticks(rotation=45, ha='right')
        else:
            return {"error": f"Unsupported graph type: {graph_type}. Supported types: 'bar', 'line'"}

        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.tight_layout()
        
        # Save to base64
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        
        image_base64 = base64.b64encode(buf.read()).decode('utf-8')
        
        plt.close()  # Important: close the figure to free memory
        
        return {"image": image_base64}

    except Exception as e:
        plt.close()  # Make sure to close the figure even if there's an error
        return {"error": f"Error creating graph: {str(e)}"}


def create_graph_from_duckdb(natural_language_request: str, db_path: str, graph_type: str = "bar", title: str = "Graph", x_label: str = "X-axis", y_label: str = "Y-axis"):
    """
    Generates a graph from DuckDB data using natural language requests.
    
    This function uses Gemini AI to convert natural language requests into SQL queries,
    executes them against a DuckDB database, and creates visualizations.
    
    Args:
        natural_language_request: User's natural language description of what to visualize
        db_path: Path to the DuckDB database file
        graph_type: Type of graph to generate ('bar', 'line', 'scatter', 'pie')
        title: Title for the graph
        x_label: Label for the x-axis
        y_label: Label for the y-axis
        
    Returns:
        Dictionary containing either the base64-encoded image or error message
    """
    try:
        # If db_path looks like just a filename, try to find it in the registry or api directory
        if not os.path.isabs(db_path) and not os.path.exists(db_path):
            file_info = file_registry.get_file_info(db_path)
            if file_info:
                db_path = file_info['db_path']
                logging.info(f"Found database in registry: {db_path}")
            else:
                # Try looking in the api directory as a fallback
                api_path = os.path.join("api", db_path)
                if os.path.exists(api_path):
                    db_path = api_path
                    logging.info(f"Found database in api directory: {db_path}")
                else:
                    return {"error": f"Database file '{db_path}' not found. Please upload the DuckDB file first."}
        
        # First, get the database schema information
        try:
            schema_info = get_table_info(db_path)
        except Exception as e:
            return {"error": f"Failed to get database schema: {str(e)}"}
        
        # Create a prompt specifically for graph data generation
        graph_prompt = f"""
        I need to create a {graph_type} chart. {natural_language_request}
        
        Please generate SQL queries that will return data suitable for visualization.
        The data should have:
        - A column for the x-axis (categories, labels, or time values)
        - A column for the y-axis (numeric values to plot)
        
        Return data that can be easily plotted in a {graph_type} chart.
        """
        
        # Generate SQL queries using Gemini AI
        try:
            sql_queries = get_sql_queries(graph_prompt, schema_info)
        except Exception as e:
            return {"error": f"Failed to generate SQL queries: {str(e)}"}
        
        if not sql_queries:
            return {"error": "No SQL queries were generated"}
        
        # Execute the SQL queries
        try:
            query_results = execute_sql_queries(sql_queries, db_path, result_format="df")
        except Exception as e:
            return {"error": f"Failed to execute SQL queries: {str(e)}"}
        
        # Find the query result with data
        data_result = None
        for result in query_results:
            if result.get("success", False) and result.get("data") and len(result["data"]) > 0:
                data_result = result
                break
        
        if not data_result:
            return {"error": "No data returned from queries"}
        
        # Convert the data to a format suitable for plotting
        data = data_result["data"]
        
        # Ensure data is in the correct format (list of dictionaries)
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            try:
                # Attempt to convert if it's a different but compatible format
                data = [dict(item) for item in data]
            except (TypeError, ValueError):
                return {"error": "Invalid data format. Expected a list of dictionaries."}

        df = pd.DataFrame(data)
        
        if df.empty:
            return {"error": "Data is empty"}

        # Set up the plot
        plt.figure(figsize=(12, 8))
        
        # Determine x and y columns
        columns = df.columns.tolist()
        
        if len(columns) < 1:
            return {"error": "Data must have at least 1 column"}
        
        # Try to identify x and y columns intelligently
        x_col = None
        y_col = None
        
        # For pie charts, we need different logic
        if graph_type == 'pie':
            # Find a categorical column and a numeric column
            for col in columns:
                if df[col].dtype in ['object', 'string'] or col.lower() in ['label', 'name', 'category', 'type']:
                    x_col = col
                elif pd.api.types.is_numeric_dtype(df[col]) and col.lower() in ['value', 'amount', 'count', 'size']:
                    y_col = col
        else:
            # For other charts, use the existing logic
            for col in columns:
                if df[col].dtype in ['object', 'string'] or col.lower() in ['label', 'name', 'category', 'month', 'date', 'time']:
                    x_col = col
                elif pd.api.types.is_numeric_dtype(df[col]) and col.lower() in ['value', 'amount', 'count', 'price', 'total']:
                    y_col = col
        
        # Fallback logic
        if x_col is None and len(columns) >= 1:
            x_col = columns[0]
        if y_col is None and len(columns) >= 2:
            # Find first numeric column
            for col in columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    y_col = col
                    break
            if y_col is None:
                y_col = columns[1]  # Fallback to second column
        
        # Handle single column case for pie charts
        if graph_type == 'pie' and y_col is None and len(columns) == 1:
            # Count occurrences of values in the single column
            value_counts = df[x_col].value_counts()
            plt.pie(value_counts.values, labels=value_counts.index, autopct='%1.1f%%')
            plt.title(title)
        else:
            # Create the plot based on graph type
            if graph_type == 'bar':
                plt.bar(df[x_col], df[y_col])
                plt.xticks(rotation=45, ha='right')
            elif graph_type == 'line':
                plt.plot(df[x_col], df[y_col], marker='o')
                plt.xticks(rotation=45, ha='right')
            elif graph_type == 'scatter':
                plt.scatter(df[x_col], df[y_col])
                plt.xticks(rotation=45, ha='right')
            elif graph_type == 'pie':
                if y_col:
                    plt.pie(df[y_col], labels=df[x_col], autopct='%1.1f%%')
                else:
                    return {"error": "Pie chart requires a numeric column for values"}
            else:
                return {"error": f"Unsupported graph type: {graph_type}. Supported types: 'bar', 'line', 'scatter', 'pie'"}

            # Add labels and title (except for pie charts which don't use axis labels)
            if graph_type != 'pie':
                plt.xlabel(x_label)
                plt.ylabel(y_label)
            plt.title(title)
        
        plt.tight_layout()
        
        # Save to base64
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        
        image_base64 = base64.b64encode(buf.read()).decode('utf-8')
        
        plt.close()  # Important: close the figure to free memory
        
        return {"image": image_base64}

    except Exception as e:
        plt.close()  # Make sure to close the figure even if there's an error
        return {"error": f"Error creating graph from DuckDB: {str(e)}"}


def execute_analytical_query_detailed(question: str, db_path: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute analytical queries against DuckDB databases with detailed results.
    
    This function provides the complete analytical workflow with all details:
    1. Extracts database schema information
    2. Generates SQL queries using AI based on the question
    3. Executes the queries against the database
    4. Generates a natural language response with insights
    
    Args:
        question: The analytical question to answer
        db_path: Path to the DuckDB database file
        session_id: Optional session ID for follow-up queries
        
    Returns:
        Dictionary containing detailed analysis results:
        {
            "success": bool,
            "answer": str,  # Natural language answer
            "sql_queries": List[str],  # Generated SQL queries
            "query_results": List[Dict],  # Raw query results
            "session_id": str,  # Session ID for follow-up queries
            "error": Optional[str]  # Error message if failed
        }
    """
    try:
        logging.info(f"Executing detailed analytical query: {question[:100]}...")
        
        # If db_path looks like just a filename, try to find it in the registry
        if not os.path.isabs(db_path) and not os.path.exists(db_path):
            file_info = file_registry.get_file_info(db_path)
            if file_info:
                db_path = file_info['db_path']
                logging.info(f"Found database in registry: {db_path}")
            else:
                result = {
                    "success": False,
                    "answer": f"Database file '{db_path}' not found. Please upload the DuckDB file first or use the list_available_databases tool to see available files.",
                    "sql_queries": [],
                    "query_results": [],
                    "session_id": session_id or "unknown",
                    "error": f"Database file not found: {db_path}"
                }
                return ensure_json_serializable(result)
        
        # Get or create database session
        session = None
        if session_id:
            session = session_manager.get_session(session_id)
        
        if session is None:
            session = session_manager.create_session(db_path, session_id)
        
        # Get schema information (cached in session)
        schema_info = session.get_schema_info()
        
        if not schema_info or not schema_info.get('tables'):
            result = {
                "success": False,
                "answer": "No tables found in the database. Please check that the database file is valid and contains data.",
                "sql_queries": [],
                "query_results": [],
                "session_id": session.session_id,
                "error": "No tables found in database"
            }
            return ensure_json_serializable(result)
        
        # Generate SQL queries using AI
        try:
            sql_queries = get_sql_queries(question, schema_info)
        except Exception as e:
            logging.error(f"SQL generation failed: {str(e)}")
            result = {
                "success": False,
                "answer": f"I couldn't generate SQL queries to answer your question. Error: {str(e)}",
                "sql_queries": [],
                "query_results": [],
                "session_id": session.session_id,
                "error": f"SQL generation failed: {str(e)}"
            }
            return ensure_json_serializable(result)
        
        if not sql_queries:
            result = {
                "success": False,
                "answer": "I couldn't generate appropriate SQL queries for your question. Please try rephrasing your question or provide more specific details.",
                "sql_queries": [],
                "query_results": [],
                "session_id": session.session_id,
                "error": "No SQL queries generated"
            }
            return ensure_json_serializable(result)
        
        # Execute SQL queries using the session's retry logic
        query_results = session.execute_queries(sql_queries)
        
        # Check if any queries succeeded
        successful_results = [r for r in query_results if r.get('success', False)]
        if not successful_results:
            error_messages = [r.get('error', 'Unknown error') for r in query_results if r.get('error')]
            combined_errors = '; '.join(error_messages)
            
            result = {
                "success": False,
                "answer": f"All SQL queries failed to execute. Errors: {combined_errors}",
                "sql_queries": sql_queries,
                "query_results": query_results,
                "session_id": session.session_id,
                "error": f"Query execution failed: {combined_errors}"
            }
            return ensure_json_serializable(result)
        
        # Generate natural language response
        try:
            response_text = generate_response(question, sql_queries, query_results)
        except Exception as e:
            logging.error(f"Response generation failed: {str(e)}")
            # Fallback to basic response
            successful_data = []
            for result in successful_results:
                if result.get('data'):
                    successful_data.extend(result['data'])
            
            if successful_data:
                response_text = f"I found {len(successful_data)} records in the database. Here are the query results: {str(successful_data[:5])}{'...' if len(successful_data) > 5 else ''}"
            else:
                response_text = "The queries executed successfully but returned no data."
        
        # Log successful execution
        logging.info(f"Detailed analytical query completed successfully for session {session.session_id}")
        
        result = {
            "success": True,
            "answer": response_text,
            "sql_queries": sql_queries,
            "query_results": query_results,
            "session_id": session.session_id,
            "error": None
        }
        
        # Ensure the result is JSON serializable
        return ensure_json_serializable(result)
        
    except Exception as e:
        logging.error(f"Unexpected error in execute_analytical_query_detailed: {str(e)}")
        result = {
            "success": False,
            "answer": f"An unexpected error occurred while processing your question: {str(e)}",
            "sql_queries": [],
            "query_results": [],
            "session_id": session_id or "unknown",
            "error": f"Unexpected error: {str(e)}"
        }
        
        # Ensure the result is JSON serializable
        return ensure_json_serializable(result)


def execute_analytical_query(question: str, db_path: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute analytical queries against DuckDB databases to answer data questions.
    
    This function provides a complete analytical workflow:
    1. Checks cache for identical questions
    2. Extracts database schema information
    3. Generates SQL queries using AI based on the question
    4. Executes the queries against the database
    5. Generates a natural language response with insights
    6. Caches the result for future identical questions
    
    Args:
        question: The analytical question to answer
        db_path: Path to the DuckDB database file
        session_id: Optional session ID for follow-up queries
        
    Returns:
        Dictionary containing the analysis results (clean format for tool responses):
        {
            "success": bool,
            "answer": str,  # Natural language answer from AI
            "session_id": str,  # Session ID for follow-up queries
            "error": Optional[str]  # Error message if failed
        }
    """
    try:
        logging.info(f"Executing analytical query: {question[:100]}...")
        
        # Check cache first for identical questions
        cached_result = query_cache.get(question, db_path)
        if cached_result is not None:
            logging.info("Returning cached result for identical question")
            return cached_result
        
        # If db_path looks like just a filename, try to find it in the registry
        if not os.path.isabs(db_path) and not os.path.exists(db_path):
            file_info = file_registry.get_file_info(db_path)
            if file_info:
                db_path = file_info['db_path']
                logging.info(f"Found database in registry: {db_path}")
            else:
                result = {
                    "success": False,
                    "answer": f"Database file '{db_path}' not found. Please upload the DuckDB file first or use the list_available_databases tool to see available files.",
                    "session_id": session_id or "unknown",
                    "error": f"Database file not found: {db_path}"
                }
                return ensure_json_serializable(result)
        
        # Get or create database session
        session = None
        if session_id:
            session = session_manager.get_session(session_id)
        
        if session is None:
            session = session_manager.create_session(db_path, session_id)
        
        # Get schema information (cached in session)
        schema_info = session.get_schema_info()
        
        if not schema_info or not schema_info.get('tables'):
            result = {
                "success": False,
                "answer": "No tables found in the database. Please check that the database file is valid and contains data.",
                "session_id": session.session_id,
                "error": "No tables found in database"
            }
            return ensure_json_serializable(result)
        
        # Generate SQL queries using AI
        try:
            sql_queries = get_sql_queries(question, schema_info)
        except Exception as e:
            logging.error(f"SQL generation failed: {str(e)}")
            result = {
                "success": False,
                "answer": f"I couldn't generate SQL queries to answer your question. Error: {str(e)}",
                "session_id": session.session_id,
                "error": f"SQL generation failed: {str(e)}"
            }
            return ensure_json_serializable(result)
        
        if not sql_queries:
            result = {
                "success": False,
                "answer": "I couldn't generate appropriate SQL queries for your question. Please try rephrasing your question or provide more specific details.",
                "session_id": session.session_id,
                "error": "No SQL queries generated"
            }
            return ensure_json_serializable(result)
        
        # Execute SQL queries using the session's retry logic
        query_results = session.execute_queries(sql_queries)
        
        # Check if any queries succeeded
        successful_results = [r for r in query_results if r.get('success', False)]
        if not successful_results:
            error_messages = [r.get('error', 'Unknown error') for r in query_results if r.get('error')]
            combined_errors = '; '.join(error_messages)
            
            result = {
                "success": False,
                "answer": f"All SQL queries failed to execute. Errors: {combined_errors}",
                "session_id": session.session_id,
                "error": f"Query execution failed: {combined_errors}"
            }
            return ensure_json_serializable(result)
        
        # Generate natural language response using AI
        try:
            response_text = generate_response(question, sql_queries, query_results)
        except Exception as e:
            logging.error(f"Response generation failed: {str(e)}")
            # Fallback to basic response
            successful_data = []
            for result in successful_results:
                if result.get('data'):
                    successful_data.extend(result['data'])
            
            if successful_data:
                response_text = f"I found {len(successful_data)} records in the database. Here are the query results: {str(successful_data[:5])}{'...' if len(successful_data) > 5 else ''}"
            else:
                response_text = "The queries executed successfully but returned no data."
        
        # Log successful execution
        logging.info(f"Analytical query completed successfully for session {session.session_id}")
        
        # Return clean response format (no raw query results or SQL queries in tool response)
        result = {
            "success": True,
            "answer": response_text,
            "session_id": session.session_id,
            "error": None
        }
        
        # Cache successful results for future identical questions
        final_result = ensure_json_serializable(result)
        query_cache.put(question, db_path, final_result)
        
        return final_result
        
    except Exception as e:
        logging.error(f"Unexpected error in execute_analytical_query: {str(e)}")
        result = {
            "success": False,
            "answer": f"An unexpected error occurred while processing your question: {str(e)}",
            "session_id": session_id or "unknown",
            "error": f"Unexpected error: {str(e)}"
        }
        
        # Ensure the result is JSON serializable
        return ensure_json_serializable(result)


def list_available_databases() -> Dict[str, Any]:
    """
    List all available DuckDB databases that have been uploaded.
    
    Returns:
        Dictionary containing information about available databases:
        {
            "success": bool,
            "databases": List[Dict],  # List of database information
            "count": int,  # Number of available databases
            "message": str  # Status message
        }
    """
    try:
        files = file_registry.list_files()
        
        if not files:
            return {
                "success": True,
                "databases": [],
                "count": 0,
                "message": "No databases are currently available. Please upload a DuckDB file first."
            }
        
        # Format database information for display
        databases = []
        for file_info in files:
            databases.append({
                "filename": file_info['filename'],
                "db_path": file_info['db_path'],
                "tables": file_info['tables'],
                "columns": file_info['columns'],
                "file_size_mb": round(file_info['file_size'] / (1024 * 1024), 2),
                "uploaded_ago_minutes": round((time.time() - file_info['registered_at']) / 60, 1)
            })
        
        return {
            "success": True,
            "databases": databases,
            "count": len(databases),
            "message": f"Found {len(databases)} available database{'s' if len(databases) != 1 else ''}."
        }
        
    except Exception as e:
        logging.error(f"Error listing available databases: {str(e)}")
        return {
            "success": False,
            "databases": [],
            "count": 0,
            "message": f"Error listing databases: {str(e)}"
        }
