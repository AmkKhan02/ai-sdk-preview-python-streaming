# Design Document

## Overview

The function call response mismatch error occurs because the current implementation doesn't properly format function responses according to Gemini's expected structure. When function calls are made, Gemini expects the responses to be sent back in a specific format that matches the original function call structure. The current code sends function responses as a list, but Gemini expects them to be formatted as content parts with proper function response objects.

## Architecture

The fix involves modifying the `stream_text` function in `api/index.py` to:

1. **Proper Response Formatting**: Format function responses according to Gemini's expected structure
2. **Content Part Structure**: Create proper content parts that match the function call parts
3. **Response Matching**: Ensure each function call has a corresponding properly formatted response

## Components and Interfaces

### Modified Components

#### 1. stream_text Function (`api/index.py`)
- **Current Issue**: Sends function responses as a raw list
- **Fix**: Format responses as proper content parts with function_response objects
- **Interface**: Maintains existing streaming response interface

#### 2. Function Response Structure
- **Current Format**: 
  ```python
  function_responses.append({
      "function_response": {
          "name": function_call.name,
          "response": result
      }
  })
  ```
- **Required Format**:
  ```python
  {
      "parts": [{
          "function_response": {
              "name": function_call.name,
              "response": result
          }
      }]
  }
  ```

## Data Models

### Function Response Content Structure
```python
# Correct structure for sending back to Gemini
response_content = {
    "parts": [
        {
            "function_response": {
                "name": str,  # Function name that was called
                "response": dict  # Function execution result
            }
        }
        # ... additional function responses as separate parts
    ]
}
```

### Function Call Processing Flow
1. **Collection Phase**: Collect all function calls from the response
2. **Execution Phase**: Execute each function call and store results
3. **Formatting Phase**: Format results into proper content structure
4. **Response Phase**: Send formatted content back to model for final response

## Error Handling

### Function Call Errors
- **Execution Errors**: Wrap in proper function_response structure with error details
- **Format Errors**: Log detailed information about structure mismatches
- **API Errors**: Provide fallback responses when Gemini API calls fail

### Debugging Improvements
- **Enhanced Logging**: Log function call structures and response formats
- **Error Context**: Include function names and call signatures in error messages
- **Response Validation**: Validate response structure before sending to Gemini

## Testing Strategy

### Unit Tests
- Test function response formatting with single function calls
- Test function response formatting with multiple function calls
- Test error handling for failed function executions

### Integration Tests
- Test complete flow from function call to final response
- Test follow-up questions with session context
- Test error scenarios and recovery

### Manual Testing
- Test the specific scenario: initial analytical query followed by follow-up question
- Verify that the error "Please ensure that the number of function response parts is equal to the number of function call parts" is resolved
- Test with different types of analytical queries to ensure robustness