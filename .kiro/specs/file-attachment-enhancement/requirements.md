# Requirements Document

## Introduction

This feature enhances the existing file attachment functionality by adding support for DuckDB files alongside CSV files. The current system has a single attachment button that processes all files through content extraction and JSON schema conversion. The enhancement will separate CSV and DuckDB file handling, with CSV files continuing to use the existing processing pipeline while DuckDB files will be sent directly to the backend for column extraction.

## Requirements

### Requirement 1

**User Story:** As a user, I want to attach CSV files through a dedicated button, so that I can continue using the existing CSV processing functionality without confusion.

#### Acceptance Criteria

1. WHEN a user clicks the CSV attachment button THEN the system SHALL only accept files with .csv extension
2. WHEN a CSV file is selected THEN the system SHALL extract the file content and convert it to JSON schema as currently implemented
3. WHEN a CSV file is processed THEN the system SHALL send the JSON schema to the backend along with the user's prompt
4. IF a non-CSV file is selected through the CSV button THEN the system SHALL display an appropriate error message

### Requirement 2

**User Story:** As a user, I want to attach DuckDB files through a dedicated button, so that I can upload database files for column analysis.

#### Acceptance Criteria

1. WHEN a user clicks the DuckDB attachment button THEN the system SHALL only accept files with .duckdb extension
2. WHEN a DuckDB file is selected THEN the system SHALL send the file directly to the backend without content extraction or JSON schema conversion
3. WHEN a DuckDB file is uploaded THEN the system SHALL maintain the binary integrity of the file during transmission
4. IF a non-DuckDB file is selected through the DuckDB button THEN the system SHALL display an appropriate error message

### Requirement 3

**User Story:** As a developer, I want the backend to process DuckDB files through a dedicated utility function, so that column information can be extracted and returned to the frontend.

#### Acceptance Criteria

1. WHEN the backend receives a DuckDB file THEN the system SHALL route it to the process_duckdb.py utility
2. WHEN process_duckdb.py processes a DuckDB file THEN the get_cols() function SHALL return a list of column names from the database
3. WHEN get_cols() encounters an invalid DuckDB file THEN the system SHALL return an appropriate error response
4. WHEN the backend processes a DuckDB file THEN the system SHALL handle file upload and temporary storage securely

### Requirement 4

**User Story:** As a user, I want clear visual distinction between CSV and DuckDB attachment buttons, so that I can easily select the appropriate file type.

#### Acceptance Criteria

1. WHEN the user views the attachment interface THEN the system SHALL display two distinct buttons for CSV and DuckDB files
2. WHEN the user hovers over each button THEN the system SHALL display tooltips indicating the accepted file types
3. WHEN the user interacts with the buttons THEN the system SHALL provide clear visual feedback for the selected file type
4. WHEN files are attached THEN the system SHALL display the file name and type in the interface

### Requirement 5

**User Story:** As a developer, I want proper error handling for both file types, so that users receive clear feedback when issues occur.

#### Acceptance Criteria

1. WHEN file upload fails THEN the system SHALL display a user-friendly error message
2. WHEN DuckDB processing fails THEN the system SHALL return specific error information about the database issue
3. WHEN CSV processing fails THEN the system SHALL maintain existing error handling behavior
4. WHEN network issues occur during file upload THEN the system SHALL provide retry options or clear failure messaging