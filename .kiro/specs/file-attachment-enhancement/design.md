# Design Document

## Overview

This design enhances the existing file attachment system by separating CSV and DuckDB file handling into distinct workflows. The current implementation uses a single attachment button that processes all files through content extraction and JSON schema conversion. The new design introduces two dedicated buttons with specialized processing pipelines while maintaining backward compatibility for existing CSV functionality.

## Architecture

### Current Architecture
- Single attachment button handles all file types
- Frontend processes files client-side (CSV parsing, DuckDB WASM processing)
- All files converted to JSON schema before sending to backend
- Backend receives only text-based data representations

### New Architecture
- **CSV Button**: Maintains existing client-side processing pipeline
- **DuckDB Button**: Direct file upload to backend with server-side processing
- **Backend Processing**: New utility module for DuckDB column extraction
- **Unified Interface**: Consistent user experience across both file types

## Components and Interfaces

### Frontend Components

#### 1. Enhanced MultimodalInput Component
**Location**: `components/multimodal-input.tsx`

**Changes**:
- Replace single attachment button with two dedicated buttons
- Add file type validation for each button
- Implement separate handling logic for CSV vs DuckDB files
- Maintain existing state management for CSV processing
- Add new state management for DuckDB file uploads

**New State Variables**:
```typescript
const [csvAttachment, setCsvAttachment] = useState<File | undefined>();
const [duckdbAttachment, setDuckdbAttachment] = useState<File | undefined>();
const [duckdbColumns, setDuckdbColumns] = useState<string[] | null>(null);
const [isDuckdbUploading, setIsDuckdbUploading] = useState(false);
```

**Button Components**:
- **CSV Button**: File input with `.csv` accept filter
- **DuckDB Button**: File input with `.duckdb,.db` accept filter

#### 2. Updated PreviewAttachment Component
**Location**: `components/preview-attachment.tsx`

**Enhancements**:
- Display file type-specific information
- Show column names for DuckDB files
- Maintain existing preview functionality for CSV files
- Add loading states for DuckDB processing

#### 3. New AttachmentButton Component
**Location**: `components/ui/attachment-button.tsx`

**Purpose**: Reusable button component for file attachments

**Props**:
```typescript
interface AttachmentButtonProps {
  fileType: 'csv' | 'duckdb';
  onFileSelect: (file: File) => void;
  disabled?: boolean;
  accept: string;
  icon: React.ReactNode;
  tooltip: string;
}
```

### Backend Components

#### 1. Enhanced FastAPI Endpoint
**Location**: `api/index.py`

**New Endpoint**: `POST /api/upload-duckdb`
- Handles multipart file uploads
- Validates DuckDB file format
- Routes to processing utility
- Returns column information

**Request/Response Format**:
```python
# Request: multipart/form-data with file
# Response:
{
  "columns": ["col1", "col2", "col3"],
  "table_name": "main_table",
  "file_size": 1024,
  "status": "success"
}
```

#### 2. New DuckDB Processing Utility
**Location**: `api/utils/process_duckdb.py`

**Core Function**:
```python
def get_cols(file_path: str) -> List[str]:
    """
    Extract column names from a DuckDB file.
    
    Args:
        file_path: Path to the DuckDB file
        
    Returns:
        List of column names from the first table
        
    Raises:
        ValueError: If file is not a valid DuckDB file
        FileNotFoundError: If file doesn't exist
    """
```

**Additional Functions**:
- `validate_duckdb_file(file_path: str) -> bool`
- `get_table_info(file_path: str) -> Dict[str, Any]`
- `cleanup_temp_file(file_path: str) -> None`

## Data Models

### Frontend Data Models

#### File Attachment State
```typescript
interface FileAttachmentState {
  csvFile?: File;
  duckdbFile?: File;
  csvData?: any[];
  duckdbColumns?: string[];
  isProcessing: boolean;
  error?: string;
}
```

#### DuckDB Response
```typescript
interface DuckDBResponse {
  columns: string[];
  tableName: string;
  fileSize: number;
  status: 'success' | 'error';
  error?: string;
}
```

### Backend Data Models

#### DuckDB Processing Result
```python
from pydantic import BaseModel
from typing import List, Optional

class DuckDBProcessingResult(BaseModel):
    columns: List[str]
    table_name: str
    file_size: int
    status: str
    error: Optional[str] = None
```

## Error Handling

### Frontend Error Handling

#### File Validation Errors
- **CSV Button**: Only accept `.csv` files
- **DuckDB Button**: Only accept `.duckdb`, `.db` files
- Display clear error messages for invalid file types
- Prevent form submission with invalid attachments

#### Upload Errors
- Network timeout handling with retry options
- File size limit validation (configurable)
- Server error response handling
- Progress indication for large file uploads

#### Processing Errors
- CSV parsing errors (maintain existing behavior)
- DuckDB column extraction errors
- Graceful fallback to file name display

### Backend Error Handling

#### File Processing Errors
```python
class DuckDBProcessingError(Exception):
    """Custom exception for DuckDB processing errors"""
    pass

# Error scenarios:
# - Invalid DuckDB file format
# - Corrupted database file
# - Empty database (no tables)
# - File system access errors
# - Memory/resource constraints
```

#### HTTP Error Responses
- `400 Bad Request`: Invalid file format
- `413 Payload Too Large`: File size exceeds limit
- `422 Unprocessable Entity`: Valid file but processing failed
- `500 Internal Server Error`: Server-side processing errors

## Testing Strategy

### Frontend Testing

#### Unit Tests
- File type validation logic
- Button state management
- Error message display
- File upload progress tracking

#### Integration Tests
- CSV file processing workflow (existing)
- DuckDB file upload workflow (new)
- Error handling scenarios
- User interaction flows

#### Component Tests
```typescript
// Example test cases
describe('MultimodalInput', () => {
  it('should only accept CSV files for CSV button');
  it('should only accept DuckDB files for DuckDB button');
  it('should display appropriate error for wrong file type');
  it('should show upload progress for DuckDB files');
});
```

### Backend Testing

#### Unit Tests
- `get_cols()` function with valid DuckDB files
- Error handling for invalid files
- File cleanup functionality
- Column extraction accuracy

#### Integration Tests
- End-to-end file upload and processing
- Error response formatting
- File size limit enforcement
- Concurrent upload handling

#### Test Data
- Sample DuckDB files with known schemas
- Corrupted DuckDB files for error testing
- Large files for performance testing
- Empty databases for edge case testing

## Implementation Considerations

### Performance
- **Frontend**: Maintain existing CSV processing performance
- **Backend**: Implement file size limits for DuckDB uploads
- **Memory**: Proper cleanup of temporary files
- **Concurrency**: Handle multiple simultaneous uploads

### Security
- File type validation on both frontend and backend
- Temporary file cleanup to prevent disk space issues
- Input sanitization for file names and paths
- Rate limiting for upload endpoints

### Scalability
- Configurable file size limits
- Temporary file storage management
- Error logging and monitoring
- Resource usage tracking

### Backward Compatibility
- Existing CSV functionality remains unchanged
- No breaking changes to current API
- Gradual migration path for users
- Fallback behavior for unsupported scenarios