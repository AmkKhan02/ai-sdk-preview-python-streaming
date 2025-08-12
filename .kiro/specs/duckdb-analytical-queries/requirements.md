# Requirements Document

## Introduction

This feature adds analytical query capabilities to the existing DuckDB file attachment system. Users can upload .duckdb files and ask natural language questions about their data, such as "How many deals did I win this month?" or "What's my win rate by industry?". The system will generate appropriate SQL queries, execute them against the database, and provide human-readable responses using AI-powered analysis.

## Requirements

### Requirement 1

**User Story:** As a user, I want to ask analytical questions about my uploaded DuckDB files in natural language, so that I can get insights from my data without writing SQL queries.

#### Acceptance Criteria

1. WHEN a user uploads a DuckDB file and asks an analytical question THEN the system SHALL generate appropriate SQL queries to answer the question
2. WHEN the system receives a natural language query THEN it SHALL use the database schema information to create contextually relevant SQL
3. WHEN multiple SQL queries are needed to answer a question THEN the system SHALL generate and execute all necessary queries
4. IF the user's question cannot be answered with the available data THEN the system SHALL provide a clear explanation of what information is missing

### Requirement 2

**User Story:** As a developer, I want a SQL generation function that converts natural language to SQL queries, so that user questions can be translated into executable database operations.

#### Acceptance Criteria

1. WHEN get_sql_queries() receives a user prompt and column information THEN it SHALL send a structured request to the Gemini API
2. WHEN the Gemini API processes the request THEN the system SHALL return a list of SQL query strings
3. WHEN generating SQL queries THEN the system SHALL consider available columns, data types, and relationships
4. IF the Gemini API fails to generate queries THEN the system SHALL return appropriate error information

### Requirement 3

**User Story:** As a developer, I want a SQL execution function that runs queries against DuckDB databases, so that generated queries can be executed and results retrieved.

#### Acceptance Criteria

1. WHEN execute_sql_queries() receives SQL queries and a database path THEN it SHALL execute each query against the DuckDB database
2. WHEN executing queries THEN the system SHALL handle both fetchall() and df() result formats as appropriate
3. WHEN query execution completes THEN the system SHALL return structured results with query metadata
4. IF query execution fails THEN the system SHALL capture and return specific error information for debugging

### Requirement 4

**User Story:** As a developer, I want a response generation function that creates human-readable answers, so that query results can be presented in natural language to users.

#### Acceptance Criteria

1. WHEN generate_response() receives user prompts, SQL queries, and results THEN it SHALL send a synthesis request to the Gemini API
2. WHEN the Gemini API processes the synthesis request THEN the system SHALL return a natural language response that answers the user's question
3. WHEN generating responses THEN the system SHALL reference specific data points and insights from the query results
4. IF response generation fails THEN the system SHALL provide fallback formatting of the raw query results

### Requirement 5

**User Story:** As a user, I want the analytical query workflow to integrate seamlessly with the existing DuckDB file upload system, so that I can analyze my data immediately after uploading.

#### Acceptance Criteria

1. WHEN a user uploads a DuckDB file THEN the system SHALL extract schema information using the existing get_cols() function
2. WHEN a user asks analytical questions THEN the system SHALL use the previously extracted schema information
3. WHEN the analytical workflow executes THEN it SHALL maintain the uploaded file's database connection throughout the process
4. IF the database connection is lost THEN the system SHALL provide clear error messaging and recovery options

### Requirement 6

**User Story:** As a user, I want comprehensive error handling for analytical queries, so that I receive helpful feedback when queries fail or data is insufficient.

#### Acceptance Criteria

1. WHEN SQL generation fails THEN the system SHALL provide specific feedback about why the query couldn't be created
2. WHEN SQL execution encounters database errors THEN the system SHALL return user-friendly error messages with suggestions
3. WHEN the AI response generation fails THEN the system SHALL fall back to presenting raw query results in a readable format
4. WHEN network issues affect AI API calls THEN the system SHALL provide retry options and clear failure messaging

### Requirement 7

**User Story:** As a user, I want to ask follow-up questions about my data, so that I can explore insights through conversational interaction.

#### Acceptance Criteria

1. WHEN a user asks follow-up questions THEN the system SHALL maintain context about the previously uploaded database
2. WHEN processing follow-up queries THEN the system SHALL reference the same database schema and connection
3. WHEN generating follow-up responses THEN the system SHALL consider previous queries and results for context
4. IF the database file is no longer available for follow-up queries THEN the system SHALL prompt the user to re-upload the file