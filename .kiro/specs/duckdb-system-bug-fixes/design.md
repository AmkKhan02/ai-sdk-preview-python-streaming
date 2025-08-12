# Design Document

## Overview

The DuckDB Analysis System has three critical issues that need comprehensive fixes:

1. **Response Format Problem**: Users see raw JSON responses instead of clean text
2. **Follow-up Question Error**: HTTP 400 errors due to function call/response mismatches
3. **Database Function Execution Errors**: DuckDB functions failing during execution

This design addresses all three issues with a unified approach that ensures clean user responses, proper session management, and robust database operations.

## Architecture

The solution involves modifications across three main components:

### 1. Response Processing Layer
- **Current Issue**: JSON structure exposed to users
- **Solution**: Extract and return only the "answer" content from internal JSON responses
- **Location**: `api/index.py` - stream_text function and analytical query endpoints

### 2. Function Call Management
- **Current Issue**: Function call/response mismatch causing HTTP 400 errors
- **Solution**: Proper consolidation of function responses and session-aware processing
- **Location**: `api/index.py` - stream_text function

### 3. Database Session Management
- **Current Issue**: Database connections not persisting, functions failing
- **Solution**: Enhanced session management with persistent connections and robust error handling
- **Location**: `api/utils/db_session.py` and `api/utils/tools.py`

## Components and Interfaces

### Modified Components

#### 1. Response Format Handler (`api/index.py`)

**Current Implementation**:
```python
# Returns full JSON structure to user
return {
    "success": true,
    "answer": "The data you uploaded contains...",
    "session_id": "74422c1b-0606-41ba-bc77-5d0a3f96e2fe",
    "error": null
}
```

**New Implementation**:
```python
def extract_clean_response(response_data: dict) -> str:
    """Extract only the answer content for user display"""
    if isinstance(response_data, dict) and 'answer' in response_data:
        return response_data['answer']
    return str(response_data)

# In analytical endpoints, return only clean text
def analyze_duckdb(request: AnalyticalQueryRequest):
    result = execute_analytical_query_detailed(...)
    if result.get('success'):
        # Return only the clean answer text
        return {"response": result['answer']}
    else:
        return {"error": result.get('error', 'Analysis failed')}
```

#### 2. Function Call Consolidation (`api/index.py`)

**Current Issue**: Multiple separate function responses causing mismatches

**Solution**: Consolidate all function responses into single content structure
```python
# Collect all function responses
function_responses = []

# After collecting all responses, send them consolidated
if function_responses:
    consolidated_response = {
        "parts": function_responses
    }
    
    # Single call to model with all responses
    final_response = chat.send_message(consolidated_response, stream=True)
```

#### 3. Enhanced Database Session Management (`api/utils/db_session.py`)

**Current Issues**: 
- Connections not persisting across requests
- Database functions failing
- Session state not maintained

**Solution**: Robust session management with connection pooling
```python
class EnhancedDuckDBSession:
    def __init__(self, db_path: str, session_id: str):
        self.connection_pool = []
        self.retry_count = 3
        self.connection_timeout = 30
    
    def get_robust_connection(self):
        """Get connection with retry logic and health checks"""
        for attempt in range(self.retry_count):
            try:
                if self.connection and self.test_connection():
                    return self.connection
                self.connection = self.create_new_connection()
                return self.connection
            except Exception as e:
                if attempt == self.retry_count - 1:
                    raise DuckDBSessionError(f"Failed to establish connection: {e}")
                time.sleep(0.5 * (attempt + 1))
    
    def execute_with_retry(self, sql: str):
        """Execute query with retry logic for failed database functions"""
        for attempt in range(self.retry_count):
            try:
                conn = self.get_robust_connection()
                return conn.execute(sql).fetchall()
            except Exception as e:
                if "database function error" in str(e).lower():
                    # Specific handling for database function errors
                    if attempt < self.retry_count - 1:
                        self.reset_connection()
                        continue
                raise
```

## Data Models

### Clean Response Structure
```python
# Internal processing maintains full structure
internal_response = {
    "success": bool,
    "answer": str,
    "sql_queries": List[str],
    "query_results": List[Dict],
    "session_id": str,
    "error": Optional[str]
}

# User-facing response contains only essential content
user_response = {
    "response": str  # Only the answer content
}
```

### Function Response Consolidation
```python
# Consolidated function response structure
consolidated_response = {
    "parts": [
        {
            "function_response": {
                "name": "execute_analytical_query",
                "response": {
                    "success": True,
                    "answer": "Clean text response only"
                }
            }
        }
    ]
}
```

### Enhanced Session Context
```python
session_context = {
    "session_id": str,
    "db_path": str,
    "connection": DuckDBConnection,
    "schema_cache": Dict[str, Any],
    "query_history": List[Dict],
    "last_accessed": float,
    "connection_health": bool,
    "retry_count": int
}
```

## Error Handling

### Response Format Errors
- **Issue**: JSON structure visible to users
- **Solution**: Always extract clean text before returning to user
- **Fallback**: If extraction fails, return error message as plain text

### Function Call Errors
- **Issue**: Mismatch between function calls and responses
- **Solution**: Validate response count matches call count before sending to Gemini
- **Fallback**: If mismatch detected, consolidate responses and retry

### Database Function Errors
- **Issue**: DuckDB functions failing with "database function error"
- **Solution**: Implement retry logic with connection reset
- **Specific Fixes**:
  - Time calculations: Use explicit CAST operations for timestamp functions
  - Date operations: Validate date formats before function calls
  - Aggregations: Add NULL handling for aggregate functions

### Session Management Errors
- **Issue**: Sessions not persisting, connections dropping
- **Solution**: Health checks and automatic reconnection
- **Cleanup**: Proper resource cleanup on session expiration

## Testing Strategy

### Unit Tests
- Test response format extraction with various input types
- Test function response consolidation with multiple function calls
- Test database session persistence across multiple queries
- Test retry logic for failed database functions

### Integration Tests
- Test complete flow: initial question → clean response (no JSON visible)
- Test follow-up questions without HTTP 400 errors
- Test time calculations and date functions work correctly
- Test session isolation between multiple users

### Manual Testing Scenarios
1. **Response Format Test**:
   - Ask analytical question
   - Verify user sees only clean text (no JSON structure)
   - Verify no text duplication

2. **Follow-up Question Test**:
   - Ask initial analytical question
   - Ask follow-up question in same session
   - Verify no HTTP 400 errors
   - Verify context is maintained

3. **Database Function Test**:
   - Ask questions requiring time calculations
   - Ask questions requiring date operations
   - Verify all database functions execute successfully

4. **Session Persistence Test**:
   - Multiple follow-up questions in same session
   - Verify database connection persists
   - Verify query history is maintained