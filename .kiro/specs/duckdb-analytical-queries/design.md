# Design Document

## Overview

This design extends the existing DuckDB file attachment system with AI-powered analytical query capabilities. Users can ask natural language questions about their uploaded databases, and the system will generate SQL queries, execute them, and provide human-readable insights. The design leverages the Gemini API for both SQL generation and response synthesis, while using DuckDB's Python API for query execution.

## Architecture

### Current Architecture Integration
- **Builds on**: Existing DuckDB file upload and column extraction system
- **Extends**: Current FastAPI backend with new analytical endpoints
- **Leverages**: Established Gemini API integration for AI capabilities
- **Maintains**: Existing file handling and security patterns

### New Analytical Query Architecture
```
User Question → SQL Generation (Gemini) → Query Execution (DuckDB) → Response Synthesis (Gemini) → Natural Language Answer
```

**Key Components**:
1. **SQL Generation Service**: Converts natural language to SQL using Gemini API
2. **Query Execution Engine**: Executes SQL against DuckDB databases
3. **Response Synthesis Service**: Converts query results to natural language
4. **Session Management**: Maintains database context for follow-up queries

## Components and Interfaces

### Backend Components

#### 1. Enhanced DuckDB Processing Utility
**Location**: `api/utils/process_duckdb.py`

**New Functions**:

```python
def get_sql_queries(user_prompt: str, columns_info: dict) -> list[str]:
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

def execute_sql_queries(sql_queries: list[str], db_path: str) -> list[dict]:
    """
    Execute SQL queries against DuckDB database.
    
    Args:
        sql_queries: List of SQL query strings to execute
        db_path: Path to the DuckDB database file
        
    Returns:
        List of dictionaries containing query results and metadata
        
    Raises:
        QueryExecutionError: If query execution fails
    """

def generate_response(user_prompt: str, sql_queries: list[str], query_results: list[dict]) -> str:
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
```

**Enhanced Schema Information**:
```python
def get_enhanced_schema_info(file_path: str) -> dict:
    """
    Get comprehensive schema information including data types, sample data, and relationships.
    
    Returns:
        {
            'tables': ['table1', 'table2'],
            'schemas': {
                'table1': [
                    {'name': 'id', 'type': 'INTEGER', 'sample_values': [1, 2, 3]},
                    {'name': 'name', 'type': 'VARCHAR', 'sample_values': ['John', 'Jane']}
                ]
            },
            'row_counts': {'table1': 1000, 'table2': 500},
            'primary_table': 'table1'
        }
    """
```

#### 2. New FastAPI Endpoint for Analytical Queries
**Location**: `api/index.py`

**New Endpoint**: `POST /api/analyze-duckdb`

```python
class AnalyticalQueryRequest(BaseModel):
    user_prompt: str
    db_file_id: str  # Reference to uploaded database
    context: Optional[dict] = None  # For follow-up queries

class AnalyticalQueryResponse(BaseModel):
    answer: str
    sql_queries: List[str]
    query_results: List[dict]
    execution_time: float
    status: str
    error: Optional[str] = None
```

**Request/Response Flow**:
```python
@app.post("/api/analyze-duckdb")
async def analyze_duckdb_data(request: AnalyticalQueryRequest):
    """
    Process analytical queries against uploaded DuckDB files.
    
    Workflow:
    1. Retrieve database file and schema information
    2. Generate SQL queries using Gemini API
    3. Execute queries against DuckDB database
    4. Generate natural language response
    5. Return comprehensive results
    """
```

#### 3. Database Session Management
**Location**: `api/utils/db_session.py`

```python
class DuckDBSession:
    """
    Manages persistent database connections and context for analytical queries.
    """
    
    def __init__(self, db_path: str, session_id: str):
        self.db_path = db_path
        self.session_id = session_id
        self.connection = None
        self.schema_info = None
        self.query_history = []
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
    
    def execute_query(self, sql: str) -> dict:
        """Execute single query with error handling and result formatting."""
    
    def cleanup(self):
        """Clean up database connection and temporary files."""
```

### AI Integration Components

#### 1. SQL Generation Service
**Integration**: Gemini API with structured prompts

**Prompt Structure**:
```python
SQL_GENERATION_PROMPT = """
You are a SQL expert. Generate SQL queries to answer the user's question based on the provided database schema.

Database Schema:
{schema_info}

User Question: {user_prompt}

Requirements:
1. Generate only valid SQL queries that can run against DuckDB
2. Use appropriate aggregations, filters, and joins
3. Consider data types and constraints
4. Return multiple queries if needed to fully answer the question
5. Use proper SQL formatting and best practices

Return your response as a JSON array of SQL query strings.
"""
```

**Response Processing**:
- Parse JSON response from Gemini
- Validate SQL syntax
- Handle generation errors gracefully
- Support multiple queries for complex questions

#### 2. Response Synthesis Service
**Integration**: Gemini API with context-aware prompts

**Prompt Structure**:
```python
RESPONSE_SYNTHESIS_PROMPT = """
You are a data analyst. Create a clear, insightful response to the user's question based on the SQL query results.

Original Question: {user_prompt}

SQL Queries Executed:
{sql_queries}

Query Results:
{query_results}

Requirements:
1. Provide a direct answer to the user's question
2. Include specific numbers and insights from the data
3. Explain any trends or patterns found
4. Use clear, non-technical language
5. Suggest follow-up questions if relevant

Generate a comprehensive but concise response.
"""
```

## Data Models

### Request/Response Models

#### Analytical Query Request
```python
class AnalyticalQueryRequest(BaseModel):
    user_prompt: str
    db_file_id: str
    context: Optional[Dict[str, Any]] = None
    include_raw_results: bool = False
    max_queries: int = 5
```

#### Query Execution Result
```python
class QueryResult(BaseModel):
    sql: str
    success: bool
    data: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    execution_time_ms: float
    error: Optional[str] = None
```

#### Analytical Response
```python
class AnalyticalQueryResponse(BaseModel):
    answer: str
    sql_queries: List[str]
    query_results: List[QueryResult]
    insights: List[str]
    follow_up_suggestions: List[str]
    execution_time: float
    status: str
    error: Optional[str] = None
```

### Database Models

#### Enhanced Schema Information
```python
class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool
    sample_values: List[Any]
    unique_count: Optional[int] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None

class TableSchema(BaseModel):
    name: str
    columns: List[ColumnInfo]
    row_count: int
    sample_rows: List[Dict[str, Any]]

class DatabaseSchema(BaseModel):
    tables: List[TableSchema]
    primary_table: str
    relationships: List[Dict[str, str]]
    file_size: int
```

## Error Handling

### SQL Generation Errors

#### Gemini API Failures
```python
class SQLGenerationError(Exception):
    """Raised when SQL generation fails."""
    
    def __init__(self, message: str, user_prompt: str, schema_info: dict):
        self.message = message
        self.user_prompt = user_prompt
        self.schema_info = schema_info
        super().__init__(message)
```

**Error Scenarios**:
- API rate limiting or quota exceeded
- Invalid schema information provided
- Ambiguous user questions
- Network connectivity issues

**Recovery Strategies**:
- Retry with simplified prompts
- Fallback to template-based SQL generation
- Request clarification from user
- Provide example questions

### Query Execution Errors

#### DuckDB Execution Failures
```python
class QueryExecutionError(Exception):
    """Raised when SQL query execution fails."""
    
    def __init__(self, message: str, sql: str, db_path: str):
        self.message = message
        self.sql = sql
        self.db_path = db_path
        super().__init__(message)
```

**Error Scenarios**:
- Invalid SQL syntax
- Missing tables or columns
- Data type mismatches
- Database corruption or access issues
- Memory or resource constraints

**Recovery Strategies**:
- SQL syntax validation before execution
- Graceful degradation with partial results
- Alternative query suggestions
- Clear error messages with suggestions

### Response Generation Errors

#### Synthesis Failures
```python
class ResponseGenerationError(Exception):
    """Raised when response synthesis fails."""
    
    def __init__(self, message: str, query_results: list):
        self.message = message
        self.query_results = query_results
        super().__init__(message)
```

**Fallback Strategies**:
- Raw data presentation with basic formatting
- Template-based response generation
- Summary statistics presentation
- Retry with simplified context

## Testing Strategy

### Unit Tests

#### SQL Generation Testing
```python
def test_get_sql_queries_basic():
    """Test basic SQL generation functionality."""
    
def test_get_sql_queries_complex():
    """Test complex queries with joins and aggregations."""
    
def test_get_sql_queries_error_handling():
    """Test error handling for invalid inputs."""
```

#### Query Execution Testing
```python
def test_execute_sql_queries_success():
    """Test successful query execution."""
    
def test_execute_sql_queries_invalid_sql():
    """Test handling of invalid SQL queries."""
    
def test_execute_sql_queries_database_errors():
    """Test database connection and access errors."""
```

#### Response Generation Testing
```python
def test_generate_response_basic():
    """Test basic response generation."""
    
def test_generate_response_complex_results():
    """Test response generation with complex query results."""
    
def test_generate_response_error_handling():
    """Test fallback response generation."""
```

### Integration Tests

#### End-to-End Workflow Testing
```python
def test_analytical_query_workflow():
    """Test complete analytical query workflow."""
    
def test_follow_up_query_context():
    """Test context preservation for follow-up queries."""
    
def test_multiple_table_analysis():
    """Test queries across multiple database tables."""
```

#### API Endpoint Testing
```python
def test_analyze_duckdb_endpoint():
    """Test the analytical query API endpoint."""
    
def test_error_response_formats():
    """Test proper error response formatting."""
    
def test_concurrent_query_handling():
    """Test handling of concurrent analytical queries."""
```

### Performance Tests

#### Query Performance
- Test query execution time with various database sizes
- Memory usage monitoring during query execution
- Concurrent query handling performance
- Large result set handling

#### AI API Performance
- Gemini API response time measurement
- Rate limiting and quota management testing
- Fallback performance when AI services are unavailable
- Context size optimization for large schemas

## Implementation Considerations

### Performance Optimization

#### Query Execution
- **Connection Pooling**: Reuse database connections for multiple queries
- **Result Caching**: Cache frequently requested query results
- **Query Optimization**: Analyze and optimize generated SQL queries
- **Memory Management**: Handle large result sets efficiently

#### AI API Usage
- **Prompt Optimization**: Minimize token usage while maintaining quality
- **Response Caching**: Cache AI responses for similar questions
- **Batch Processing**: Group multiple queries when possible
- **Context Management**: Optimize context size for follow-up queries

### Security Considerations

#### Database Security
- **Read-Only Access**: Ensure all database connections are read-only
- **SQL Injection Prevention**: Validate and sanitize all generated SQL
- **File Access Control**: Restrict database file access to authorized sessions
- **Resource Limits**: Implement query timeout and resource usage limits

#### Data Privacy
- **Temporary File Cleanup**: Ensure uploaded databases are properly cleaned up
- **Session Isolation**: Prevent cross-session data access
- **Logging Restrictions**: Avoid logging sensitive query results
- **API Key Security**: Secure storage and usage of AI API keys

### Scalability Planning

#### Horizontal Scaling
- **Stateless Design**: Design services to be stateless where possible
- **Database Session Management**: Implement distributed session storage
- **Load Balancing**: Support multiple backend instances
- **Resource Monitoring**: Track resource usage and performance metrics

#### Vertical Scaling
- **Memory Optimization**: Efficient handling of large databases and results
- **CPU Optimization**: Optimize query execution and AI processing
- **Storage Management**: Efficient temporary file and cache management
- **Connection Management**: Optimize database connection usage

## Future Enhancements

### Advanced Analytics
- **Statistical Analysis**: Built-in statistical functions and insights
- **Data Visualization**: Integration with charting libraries
- **Trend Analysis**: Time-series analysis capabilities
- **Predictive Analytics**: Basic forecasting and prediction features

### User Experience
- **Query Suggestions**: AI-powered query suggestions based on data
- **Interactive Exploration**: Guided data exploration workflows
- **Export Capabilities**: Export results to various formats
- **Collaboration Features**: Share queries and results with team members