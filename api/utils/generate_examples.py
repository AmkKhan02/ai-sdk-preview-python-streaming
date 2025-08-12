"""
Generate targeted examples and follow-up questions for specific data insights.

This module provides functionality to generate concise, targeted footnotes and 
follow-up questions based on the type of question asked. Instead of comprehensive
analysis, it focuses on key stats and notable datapoints with engineered responses
for different question categories.
"""

import os
import json
import logging
import re
from typing import Dict, Any, Optional, Union, List, Tuple
import google.generativeai as genai
from .process_duckdb import get_sql_queries, execute_sql_queries, get_table_info
from .file_registry import file_registry

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

# Question type patterns and their handlers
QUESTION_HANDLERS = {
    'leads_volume': {
        'patterns': [r'most leads', r'lead count', r'number of leads', r'leads.*month', r'leads.*time'],
        'queries': ['outlier_detection', 'bulk_submissions', 'notable_individual_leads', 'temporal_clustering']
    },
    'win_rate': {
        'patterns': [r'win rate', r'conversion', r'deals won', r'success rate', r'close rate'],
        'queries': ['exceptional_performers', 'conversion_outliers', 'deal_anomalies', 'segment_deviations']
    },
    'marketing_source': {
        'patterns': [r'marketing source', r'traffic source', r'channel', r'campaign'],
        'queries': ['source_outliers', 'campaign_spikes', 'channel_anomalies', 'performance_deviations']
    },
    'industry_analysis': {
        'patterns': [r'industry', r'sector', r'vertical', r'business type'],
        'queries': ['industry_outliers', 'sector_concentrations', 'vertical_anomalies', 'business_type_spikes']
    },
    'temporal_analysis': {
        'patterns': [r'month', r'quarter', r'year', r'time', r'when', r'period'],
        'queries': ['temporal_outliers', 'seasonal_anomalies', 'period_spikes', 'time_clustering']
    }
}


def generate_specific_examples(user_prompt: str, model_response: Union[str, Dict[str, Any]], db_path: Optional[str] = None) -> str:
    """
    Generate targeted footnotes highlighting outliers and notable datapoints.
    
    This function:
    1. Classifies the user's question into a specific category
    2. Generates targeted SQL queries that specifically identify outliers and anomalies
    3. Executes queries to surface notable individual records and statistical deviations
    4. Creates concise footnotes highlighting exceptional datapoints and patterns
    
    Args:
        user_prompt: The original user question/prompt
        model_response: The model's response (could be text or dict with image data)
        db_path: Optional database path. If not provided, will try to find the most recent database
        
    Returns:
        String containing targeted footnotes with outliers and notable insights
    """
    try:
        print("Hello - Generate Specific Examples")
        logging.info("="*80)
        logging.info("STARTING TARGETED QUESTION ANALYSIS")
        logging.info("="*80)
        logging.info(f"📝 USER PROMPT: {user_prompt}")
        logging.info(f"🤖 MODEL RESPONSE: {str(model_response)[:200]}{'...' if len(str(model_response)) > 200 else ''}")
        logging.info(f"💾 DB PATH PROVIDED: {db_path}")
        
        # If no db_path provided, try to find the most recent database
        if not db_path:
            logging.info("🔍 No db_path provided, searching for most recent database...")
            db_path = _find_most_recent_database()
            if not db_path:
                logging.warning("❌ No database path provided and couldn't find recent database")
                return ""
            logging.info(f"✅ Found recent database: {db_path}")
        
        # Validate database path
        if not os.path.exists(db_path):
            logging.info(f"🔍 Database file not found at {db_path}, checking file registry...")
            file_info = file_registry.get_file_info(db_path)
            if file_info:
                db_path = file_info['db_path']
                logging.info(f"✅ Found database in registry: {db_path}")
            else:
                logging.warning(f"❌ Database file not found: {db_path}")
                return ""
        
        # Get database schema for context
        try:
            logging.info("📊 Getting database schema information...")
            schema_info = get_table_info(db_path)
            logging.info(f"✅ Schema retrieved: {len(schema_info.get('tables', []))} tables found")
        except Exception as e:
            logging.error(f"❌ Failed to get database schema: {str(e)}")
            return ""
        
        # Extract text from model response for analysis
        response_text = _extract_response_text(model_response)
        
        # Step 1: Classify the question type
        logging.info("🎯 STEP 1: Classifying question type...")
        question_type = _classify_question_type(user_prompt)
        logging.info(f"✅ Question classified as: {question_type}")
        
        # Step 2: Generate targeted queries for this question type
        logging.info("🔍 STEP 2: Generating targeted queries...")
        targeted_queries = _generate_targeted_queries(user_prompt, response_text, schema_info, question_type)
        
        if not targeted_queries:
            logging.warning("❌ No targeted queries generated")
            return ""
        
        logging.info(f"✅ Generated {len(targeted_queries)} targeted queries")
        
        # Step 3: Execute the targeted queries
        logging.info("🚀 STEP 3: Executing targeted queries...")
        query_results = _execute_investigative_queries(targeted_queries, db_path)
        
        if not query_results:
            logging.warning("❌ No query results obtained")
            return ""
        
        logging.info(f"✅ Executed queries successfully: {len(query_results)} results with data")
        
        # Step 4: Generate concise footnotes and follow-up questions
        logging.info("📝 STEP 4: Generating targeted footnotes and follow-ups...")
        footnotes = _generate_targeted_footnotes(user_prompt, response_text, query_results, question_type)
        
        if footnotes:
            logging.info("✅ TARGETED FOOTNOTES GENERATED SUCCESSFULLY")
            logging.info(f"📄 Footnote length: {len(footnotes)} characters")
        else:
            logging.warning("❌ No footnotes generated")
        
        logging.info("="*80)
        logging.info("COMPLETED TARGETED QUESTION ANALYSIS")
        logging.info("="*80)
        
        return footnotes
        
    except Exception as e:
        logging.error(f"❌ ERROR in generate_specific_examples: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
        return ""  # Return empty footnote on error to avoid breaking the main response


def _classify_question_type(user_prompt: str) -> str:
    """
    Classify the user's question into a specific category for targeted response.
    
    Args:
        user_prompt: The user's original question
        
    Returns:
        Question type key from QUESTION_HANDLERS
    """
    user_prompt_lower = user_prompt.lower()
    
    # Check each question type pattern
    for question_type, config in QUESTION_HANDLERS.items():
        for pattern in config['patterns']:
            if re.search(pattern, user_prompt_lower):
                logging.info(f"🎯 Matched pattern '{pattern}' for type '{question_type}'")
                return question_type
    
    # Default to temporal analysis if no specific pattern matches
    logging.info("🎯 No specific pattern matched, defaulting to temporal_analysis")
    return 'temporal_analysis'


def _generate_targeted_queries(user_prompt: str, response_text: str, schema_info: Dict[str, Any], question_type: str) -> List[str]:
    """
    Generate targeted SQL queries based on the classified question type.
    
    Args:
        user_prompt: Original user question
        response_text: Model's response text
        schema_info: Database schema information
        question_type: Classified question type
        
    Returns:
        List of targeted SQL queries
    """
    try:
        logging.info(f"🎯 Generating queries for question type: {question_type}")
        model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Get the handler configuration
        handler_config = QUESTION_HANDLERS.get(question_type, QUESTION_HANDLERS['temporal_analysis'])
        
        # Format schema information
        schema_text = _format_schema_for_prompt(schema_info)
        
        prompt = f"""Generate 2-3 targeted SQL queries for a {question_type} analysis that SPECIFICALLY identify outliers and notable datapoints.

User's Question: {user_prompt}
Model's Response: {response_text}

Database Schema:
{schema_text}

Question Type: {question_type}
Focus Areas: {', '.join(handler_config.get('queries', []))}

Generate queries that SPECIFICALLY target:
1. **Outlier Detection**: Find unusually high/low values, statistical anomalies
2. **Notable Individual Records**: Identify specific leads, companies, or events that stand out
3. **Concentration Analysis**: Find where activity is clustered (dates, sources, industries)
4. **Deviation Analysis**: Compare top performers vs averages to show what's exceptional

CRITICAL DuckDB SQL Requirements:
- Each query MUST be designed to surface outliers or notable datapoints
- Use ONLY simple window functions without nested window functions
- Use basic aggregations like COUNT, SUM, AVG, MIN, MAX
- Use PERCENTILE_CONT for percentile calculations
- Use ROW_NUMBER() OVER (ORDER BY column) for ranking (no complex window definitions)
- Use GROUP BY and HAVING for filtering aggregated results
- AVOID: Complex window function expressions, nested window functions, or window functions in window definitions

SAFE Example query patterns:
- Find top performers: SELECT *, ROW_NUMBER() OVER (ORDER BY metric DESC) as rank FROM table ORDER BY metric DESC LIMIT 10
- Find high-volume periods: SELECT date, COUNT(*) as count FROM table GROUP BY date HAVING COUNT(*) > 5 ORDER BY count DESC
- Find percentile thresholds: WITH stats AS (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY value) as p90 FROM table) SELECT * FROM table, stats WHERE value > p90
- Basic clustering: SELECT category, COUNT(*) as count FROM table GROUP BY category ORDER BY count DESC

IMPORTANT: Keep window functions simple and avoid nesting them or using them in complex expressions.

Return as JSON array of query strings:
["SELECT query1...", "SELECT query2...", "SELECT query3..."]"""

        response = model.generate_content(prompt)
        
        if not response.text:
            logging.warning("❌ Empty response from Gemini for targeted queries")
            return []
        
        # Parse JSON response
        try:
            cleaned_text = response.text.strip()
            start_idx = cleaned_text.find('[')
            end_idx = cleaned_text.rfind(']')
            
            if start_idx == -1 or end_idx == -1:
                logging.error("❌ No JSON array found in response")
                return []
            
            json_text = cleaned_text[start_idx:end_idx + 1]
            queries = json.loads(json_text)
            
            if not isinstance(queries, list):
                logging.warning(f"❌ Response is not a JSON array, got: {type(queries)}")
                return []
            
            # Validate and filter queries
            valid_queries = []
            for query in queries:
                if isinstance(query, str) and query.strip():
                    cleaned_query = query.strip()
                    # Basic validation to catch obvious syntax errors
                    if _validate_sql_syntax(cleaned_query):
                        valid_queries.append(cleaned_query)
                    else:
                        logging.warning(f"❌ Skipping invalid SQL query: {cleaned_query[:100]}...")
            
            logging.info(f"✅ Generated {len(valid_queries)} targeted queries")
            return valid_queries
            
        except json.JSONDecodeError as e:
            logging.error(f"❌ Failed to parse JSON response: {str(e)}")
            return []
            
    except Exception as e:
        logging.error(f"❌ Error generating targeted queries: {str(e)}")
        return []


def _generate_targeted_footnotes(user_prompt: str, response_text: str, query_results: List[Dict], question_type: str) -> str:
    """
    Generate concise, targeted footnotes with key stats and follow-up questions.
    
    Args:
        user_prompt: Original user question
        response_text: Model's response text
        query_results: Results from targeted queries
        question_type: Classified question type
        
    Returns:
        Formatted footnotes with stats and follow-up questions
    """
    try:
        if not query_results:
            return ""
        
        model = genai.GenerativeModel('gemini-2.5-flash-lite')
        handler_config = QUESTION_HANDLERS.get(question_type, QUESTION_HANDLERS['temporal_analysis'])
        
        # Format query results concisely
        results_summary = _format_query_results_concisely(query_results)
        
        prompt = f"""Create a CONCISE footnote for this {question_type} question that highlights OUTLIERS and NOTABLE DATAPOINTS.

User's Question: {user_prompt}
Main Response: {response_text}

Supporting Data (contains outliers and notable datapoints):
{results_summary}

Generate a brief footnote that SPECIFICALLY highlights:
1. **Statistical outliers** - values significantly above/below average
2. **Notable individual records** - specific companies, dates, or events that stand out
3. **Concentration patterns** - where activity is clustered or unusually concentrated
4. **Deviation insights** - how much top performers differ from typical patterns

Format as:
**Notable insights:**
• [Specific outlier with exact numbers and context]
• [Individual record or event that stands out]
• [Pattern or concentration that's unusual]

Requirements:
- Use EXACT numbers from the supporting data
- Highlight what makes these datapoints exceptional
- Focus on anomalies, not just general statistics
- Keep it concise - maximum 3 bullet points
- DO NOT include follow-up questions"""

        response = model.generate_content(prompt)
        
        if not response.text:
            return ""
        
        footnotes = response.text.strip()
        
        return f"\n\n{footnotes}"
        
    except Exception as e:
        logging.error(f"❌ Error generating targeted footnotes: {str(e)}")
        return ""


def _format_query_results_concisely(query_results: List[Dict]) -> str:
    """
    Format query results concisely for footnote generation.
    
    Args:
        query_results: List of query result dictionaries
        
    Returns:
        Concise formatted results string
    """
    if not query_results:
        return "No data available"
    
    formatted_results = []
    
    for i, result in enumerate(query_results, 1):
        data = result.get('data', [])
        row_count = result.get('row_count', 0)
        
        if data and row_count > 0:
            # Show only top 3 rows for conciseness
            sample_size = min(3, len(data))
            result_summary = f"Query {i} - {row_count} rows:"
            
            for j, row in enumerate(data[:sample_size]):
                if isinstance(row, dict):
                    row_items = []
                    for key, value in row.items():
                        row_items.append(f"{key}: {value}")
                    result_summary += f"\n  {{{', '.join(row_items)}}}"
            
            formatted_results.append(result_summary)
    
    return "\n\n".join(formatted_results)


def _find_most_recent_database() -> Optional[str]:
    """
    Find the most recently uploaded database from the file registry.
    
    Returns:
        Path to the most recent database file, or None if no databases found
    """
    try:
        files = file_registry.list_files()
        if not files:
            return None
        
        # Find the most recently registered file
        most_recent = max(files, key=lambda x: x.get('registered_at', 0))
        return most_recent.get('db_path')
        
    except Exception as e:
        logging.error(f"Error finding most recent database: {str(e)}")
        return None


def _extract_response_text(model_response: Union[str, Dict[str, Any]]) -> str:
    """
    Extract text content from model response for analysis.
    
    Args:
        model_response: The model's response (string or dict)
        
    Returns:
        Extracted text content
    """
    if isinstance(model_response, str):
        return model_response
    elif isinstance(model_response, dict):
        # If it's a dict, look for common text fields
        text_fields = ['answer', 'text', 'content', 'response']
        for field in text_fields:
            if field in model_response and isinstance(model_response[field], str):
                return model_response[field]
        
        # If it contains an image, note that it's a visualization
        if 'image' in model_response:
            return "Data visualization generated showing trends and patterns in the dataset."
        
        # Fallback to string representation
        return str(model_response)
    else:
        return str(model_response)


# Removed old comprehensive query generation function - replaced with targeted approach


def _execute_investigative_queries(queries: list, db_path: str) -> list:
    """
    Execute the investigative SQL queries and return results.
    
    Args:
        queries: List of SQL query strings
        db_path: Path to the database file
        
    Returns:
        List of query results
    """
    try:
        if not queries:
            logging.warning("❌ No queries provided for execution")
            return []
        
        logging.info(f"🚀 Executing {len(queries)} SQL queries against database: {db_path}")
        
        # Execute queries using the existing function
        results = execute_sql_queries(queries, db_path, result_format="fetchall")
        
        logging.info(f"📊 Query execution completed. Processing {len(results)} results...")
        
        # Filter to only successful results with data
        successful_results = []
        for i, result in enumerate(results, 1):
            success = result.get('success', False)
            data = result.get('data', [])
            row_count = result.get('row_count', 0)
            error = result.get('error')
            sql = result.get('sql', '')
            
            logging.info(f"   📋 Query {i} Results:")
            logging.info(f"      SQL: {sql[:80]}{'...' if len(sql) > 80 else ''}")
            logging.info(f"      Success: {success}")
            logging.info(f"      Row count: {row_count}")
            
            if success and data and len(data) > 0:
                successful_results.append(result)
                logging.info(f"      ✅ Added to successful results")
                
                # Log sample data for debugging
                if len(data) > 0 and isinstance(data[0], dict):
                    sample_row = data[0]
                    sample_preview = {k: str(v)[:30] + ('...' if len(str(v)) > 30 else '') for k, v in sample_row.items()}
                    logging.info(f"      📄 Sample row: {sample_preview}")
            else:
                if error:
                    logging.warning(f"      ❌ Query failed: {error}")
                else:
                    logging.warning(f"      ⚠️  Query returned no data")
        
        logging.info(f"✅ Successfully processed queries: {len(successful_results)} out of {len(results)} have usable data")
        return successful_results
        
    except Exception as e:
        logging.error(f"❌ Error executing investigative queries: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
        return []


# Removed old comprehensive footnote generation function - replaced with targeted approach


def _validate_sql_syntax(query: str) -> bool:
    """
    Basic validation to catch common SQL syntax errors.
    
    Args:
        query: SQL query string to validate
        
    Returns:
        True if query passes basic validation, False otherwise
    """
    try:
        query_upper = query.upper()
        
        # Check for obvious syntax issues
        problematic_patterns = [
            # Window functions in window definitions - more comprehensive patterns
            r'OVER\s*\([^)]*OVER\s*\(',                    # Nested OVER clauses
            r'COUNT\s*\([^)]*OVER',                        # COUNT with OVER inside parentheses
            r'SUM\s*\([^)]*OVER',                          # SUM with OVER inside parentheses
            r'AVG\s*\([^)]*OVER',                          # AVG with OVER inside parentheses
            r'PARTITION\s+BY\s+[^)]*\bOVER\b',            # PARTITION BY with OVER clause inside
            r'ROW_NUMBER\s*\(\s*\)\s*OVER\s*\([^)]*OVER', # ROW_NUMBER with nested OVER
            r'OVER\s*\([^)]*ROW_NUMBER\s*\(',             # OVER with ROW_NUMBER inside
            r'COUNT\s*\([^)]*SUM\s*\([^)]*OVER',          # Nested aggregations with OVER
            r'COUNT\s*\(\s*SUM\s*\([^)]*\)\s*OVER',       # COUNT(SUM(...) OVER pattern
            r'SUM\s*\([^)]*\)\s*OVER\s*\([^)]*\)',        # SUM(...) OVER pattern inside other functions
        ]
        
        for pattern in problematic_patterns:
            if re.search(pattern, query_upper):
                logging.warning(f"❌ Query contains problematic pattern: {pattern}")
                return False
        
        # Basic structure validation
        if not query_upper.strip().startswith('SELECT') and not query_upper.strip().startswith('WITH'):
            logging.warning("❌ Query must start with SELECT or WITH")
            return False
            
        # Check for balanced parentheses
        if query.count('(') != query.count(')'):
            logging.warning("❌ Query has unbalanced parentheses")
            return False
            
        return True
        
    except Exception as e:
        logging.warning(f"❌ Error validating SQL syntax: {str(e)}")
        return False


def _format_schema_for_prompt(schema_info: Dict[str, Any]) -> str:
    """
    Format database schema information for use in prompts.
    
    Args:
        schema_info: Schema information from get_table_info()
        
    Returns:
        Formatted schema string
    """
    if not schema_info or 'tables' not in schema_info:
        return "No schema information available"
    
    schema_lines = []
    
    for table_name in schema_info['tables']:
        row_count = schema_info.get('row_counts', {}).get(table_name, 0)
        schema_lines.append(f"Table: {table_name} ({row_count:,} rows)")
        
        if table_name in schema_info.get('schemas', {}):
            columns = schema_info['schemas'][table_name]
            schema_lines.append("  Columns:")
            for col in columns:
                col_name = col.get('name', 'unknown')
                col_type = col.get('type', 'unknown')
                sample_values = col.get('sample_values', [])
                
                col_line = f"    - {col_name}: {col_type}"
                if sample_values:
                    sample_str = ", ".join(str(v) for v in sample_values[:3])
                    col_line += f" (examples: {sample_str})"
                
                schema_lines.append(col_line)
        
        schema_lines.append("")
    
    return "\n".join(schema_lines)


# Removed old comprehensive query results formatting - replaced with concise version above
