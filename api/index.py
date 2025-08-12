import os
import json
import uuid
import logging
import tempfile
from typing import List, Dict, Any
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request as FastAPIRequest, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
import google.generativeai as genai
from .utils.prompt import ClientMessage, convert_to_gemini_messages
from .utils.tools import get_current_weather, create_graph, execute_analytical_query, execute_analytical_query_detailed, list_available_databases
from .utils.process_duckdb import process_duckdb_file, DuckDBProcessingError, extract_clean_response
from .utils.file_registry import file_registry

load_dotenv(".env.local")

app = FastAPI()

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

class Request(BaseModel):
    messages: List[ClientMessage]

class DuckDBProcessingResult(BaseModel):
    columns: List[str]
    table_name: str
    all_tables: List[str]
    file_size: int
    status: str
    db_path: str = None  # Path to the database file for analytical queries
    error: str = None

class AnalyticalQueryRequest(BaseModel):
    question: str
    db_path: str
    session_id: str = None

available_tools = {
    "get_current_weather": get_current_weather,
    "create_graph": create_graph,
    "execute_analytical_query": execute_analytical_query,
    "execute_analytical_query_detailed": execute_analytical_query_detailed,
    "list_available_databases": list_available_databases,
}

def to_serializable(obj):
    """Recursively converts Gemini's internal types to JSON serializable formats."""
    # Handle Gemini's Struct type (common in function call args)
    if hasattr(obj, '_pb'):  # Protocol buffer object
        if hasattr(obj, 'keys') and hasattr(obj, 'values'):
            return {key: to_serializable(obj[key]) for key in obj.keys()}
        elif hasattr(obj, '__iter__'):
            return [to_serializable(item) for item in obj]
    
    # Handle dictionary-like objects
    if hasattr(obj, 'items'):
        return {key: to_serializable(value) for key, value in obj.items()}
    
    # Handle list-like objects (including RepeatedComposite)
    if isinstance(obj, list) or type(obj).__name__ in ['RepeatedCompositeFieldContainer', 'RepeatedComposite']:
        return [to_serializable(item) for item in obj]
    
    # Handle basic types
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    
    # Fallback: try to convert to string
    try:
        return str(obj)
    except:
        return None

# Define tools for Gemini
gemini_tools = [
    {
        "function_declarations": [
            {
                "name": "get_current_weather",
                "description": "Get the current weather at a location",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "latitude": {
                            "type": "number",
                            "description": "The latitude of the location",
                        },
                        "longitude": {
                            "type": "number",
                            "description": "The longitude of the location",
                        },
                    },
                    "required": ["latitude", "longitude"],
                },
            },
            {
                "name": "create_graph",
                "description": "Create a graph from a list of data points",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "data": {
                            "type": "array",
                            "description": "A list of dictionaries representing the data points",
                            "items": {
                                "type": "object"
                            }
                        },
                        "graph_type": {
                            "type": "string",
                            "description": "The type of graph to generate (e.g., 'bar', 'line')",
                        },
                        "title": {
                            "type": "string",
                            "description": "The title of the graph",
                        },
                        "x_label": {
                            "type": "string",
                            "description": "The label for the x-axis",
                        },
                        "y_label": {
                            "type": "string",
                            "description": "The label for the y-axis",
                        },
                    },
                    "required": ["data", "graph_type"],
                },
            },
            {
                "name": "execute_analytical_query",
                "description": "Execute analytical queries against uploaded DuckDB databases to answer data questions",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "The analytical question to answer using the database",
                        },
                        "db_path": {
                            "type": "string",
                            "description": "Path to the DuckDB database file (use the path from the upload response)",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Optional session ID for follow-up queries to maintain context",
                        },
                    },
                    "required": ["question", "db_path"],
                },
            },
            {
                "name": "list_available_databases",
                "description": "List all available DuckDB databases that have been uploaded and can be used for analysis",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            }
        ]
    }
]

def stream_text(messages_data: Dict, protocol: str = 'data'):
    try:
        logging.info(f"Data sent to Gemini: {messages_data}")
        
        # Default system instruction for analytical capabilities
        default_system_instruction = """You are an AI assistant with advanced data analysis capabilities. 

When users upload DuckDB database files, you can analyze them using the execute_analytical_query tool. This tool:
- Automatically generates and executes SQL queries based on the user's question
- Uses AI to create a natural language response with insights
- Returns only the essential answer without exposing technical details
- Handles follow-up questions using session context
- Ensures DuckDB-compatible SQL syntax (uses 'julian' function instead of 'JULIANDAY')

IMPORTANT: When a user asks questions about their uploaded data:

1. If you don't know what databases are available, use the list_available_databases tool first to see what files have been uploaded.

2. Use the execute_analytical_query tool with:
   - question: The user's question about the data
   - db_path: You can use either the full database path OR just the filename (e.g., "leads_data.duckdb")
   - session_id: Optional, for follow-up queries to maintain context

The tool will automatically:
- Extract database schema information
- Generate appropriate SQL queries using AI (with DuckDB-compatible syntax)
- Execute the queries against the database
- Generate a comprehensive natural language response

You can answer questions like:
- "Describe the data I've uploaded"
- "What is the most common marketing source?"
- "How many leads converted to deals?"
- "What's the average time from form submission to deal closure?"
- "Show me conversion rates by industry"

The tool returns a clean response with just the answer. You should present this answer directly to the user as your analysis of their data.

If no databases are available, ask the user to upload their DuckDB file first.

Always use the analytical query tool when users ask questions about their uploaded database data."""

        # Use provided system instruction or default
        system_instruction = messages_data.get("system_instruction")
        if system_instruction:
            # Combine with default instruction
            combined_instruction = f"{default_system_instruction}\n\nAdditional instructions:\n{system_instruction}"
        else:
            combined_instruction = default_system_instruction
        
        # Initialize the model
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            tools=gemini_tools,
            system_instruction=combined_instruction
        )

        # Start chat with history
        contents = messages_data.get("contents", [])
        
        # If we have previous messages, use them as history
        if len(contents) > 1:
            chat = model.start_chat(history=contents[:-1])
            # Get the last message as the current prompt
            current_message = contents[-1]["parts"][0]["text"] if contents else ""
        else:
            chat = model.start_chat()
            current_message = contents[0]["parts"][0]["text"] if contents else ""

        # Generate streaming response
        response = chat.send_message(
            current_message,
            stream=True,
        )

        # Track function calls to avoid duplicates
        processed_function_calls = set()
        function_responses = []

        # First pass: collect all function calls and execute them
        for chunk in response:
            logging.info(f"Raw response from Gemini: {chunk}")
            # Handle text content
            if hasattr(chunk, 'text') and chunk.text:
                yield f'0:{json.dumps(chunk.text)}\n'
            
            # Handle function calls
            if hasattr(chunk, 'candidates') and chunk.candidates:
                candidate = chunk.candidates[0]
                if hasattr(candidate, 'content') and candidate.content.parts:
                    for part in candidate.content.parts:
                        if hasattr(part, 'function_call'):
                            function_call = part.function_call
                            
                            # Create a unique identifier for this function call
                            call_signature = f"{function_call.name}_{hash(str(function_call.args))}"
                            
                            # Skip if we've already processed this exact function call
                            if call_signature in processed_function_calls:
                                continue
                                
                            processed_function_calls.add(call_signature)
                            tool_call_id = str(uuid.uuid4())
                            
                            # Convert args from struct to dict
                            args = {}
                            if hasattr(function_call, 'args') and function_call.args is not None:
                                args = to_serializable(function_call.args)
                            
                            # Yield tool call
                            yield f'9:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)}}}\n'
                            logging.info(f"Executing tool: {function_call.name} with args: {args}")
                            
                            # Execute the function
                            if function_call.name in available_tools:
                                try:
                                    result = available_tools[function_call.name](**args)
                                    logging.info(f"Tool result: {result}")
                                    
                                    # For analytical query functions, extract clean response
                                    if function_call.name in ['execute_analytical_query', 'execute_analytical_query_detailed']:
                                        clean_result = extract_clean_response(result)
                                        # Create a clean response structure for the model
                                        model_result = {
                                            "success": result.get('success', False),
                                            "answer": clean_result,
                                            "session_id": result.get('session_id', 'unknown')
                                        }
                                    else:
                                        model_result = result
                                    
                                    # Yield tool result (this goes to the UI)
                                    yield f'a:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)},"result":{json.dumps(model_result)}}}\n'
                                    
                                    # Store function response for sending back to model
                                    function_responses.append({
                                        "function_response": {
                                            "name": function_call.name,
                                            "response": model_result
                                        }
                                    })
                                                                        
                                except Exception as e:
                                    error_result = {"error": str(e)}
                                    yield f'a:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)},"result":{json.dumps(error_result)}}}\n'
                                    
                                    # Store error response
                                    function_responses.append({
                                        "function_response": {
                                            "name": function_call.name,
                                            "response": error_result
                                        }
                                    })

        # If we have function responses, send them all back to the model in one go
        if function_responses:
            # Extract clean answers from function responses
            clean_parts = []
            
            for func_resp in function_responses:
                if 'function_response' in func_resp:
                    response_data = func_resp['function_response']['response']
                    
                    # For analytical queries, use just the answer
                    if func_resp['function_response']['name'] in ['execute_analytical_query', 'execute_analytical_query_detailed']:
                        if isinstance(response_data, dict) and 'answer' in response_data:
                            clean_parts.append(response_data['answer'])
                        else:
                            clean_parts.append(str(response_data))
                    else:
                        # For other functions, convert to string
                        clean_parts.append(str(response_data))
            
            consolidated_response = {
                "parts": clean_parts + ["Based on the analysis above, provide a natural language response to the user. Do not include any JSON, code blocks, or structured data."]
            }
            
            logging.info(f"Sending consolidated function responses to model: {len(function_responses)} responses")
            logging.info(f"Consolidated response structure: {consolidated_response}")
            
            # Send all function responses back to the model for a final, consolidated response
            final_response = chat.send_message(
                consolidated_response,
                stream=True,
            )
            
            # Stream the final text response from the model
            for chunk in final_response:
                if hasattr(chunk, 'text') and chunk.text:
                    yield f'0:{json.dumps(chunk.text)}\n'
            
            # After streaming the final response, end the stream
            yield 'e:{"finishReason":"stop","usage":{"promptTokens":0,"completionTokens":0},"isContinued":false}\n'
        else:
            # If there were no function calls, end the stream now
            yield 'e:{"finishReason":"stop","usage":{"promptTokens":0,"completionTokens":0},"isContinued":false}\n'

    except Exception as e:
        # Handle errors
        logging.error(f"An error occurred in stream_text: {str(e)}")
        yield f'0:{json.dumps(f"Error: {str(e)}")}\n'
        yield 'e:{"finishReason":"error","usage":{"promptTokens":0,"completionTokens":0},"isContinued":false}\n'
        
@app.post("/api/upload-duckdb")
async def upload_duckdb_file(file: UploadFile = File(...)):
    """
    Upload and process a DuckDB file to extract column information.
    
    Args:
        file: The uploaded DuckDB file
        
    Returns:
        JSON response with column information and processing status
        
    Raises:
        HTTPException: For various error conditions
    """
    # File size validation (10MB limit)
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB in bytes
    
    # Validate file extension
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    allowed_extensions = ['.duckdb', '.db']
    file_extension = os.path.splitext(file.filename.lower())[1]
    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid file type. Only {', '.join(allowed_extensions)} files are allowed"
        )
    
    # Create temporary file
    temp_file_path = None
    try:
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        if len(file_content) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413, 
                detail=f"File too large. Maximum size is {MAX_FILE_SIZE // (1024*1024)}MB"
            )
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name
        
        # Process the DuckDB file
        try:
            logging.info(f"Processing DuckDB file: {temp_file_path}, size: {len(file_content)} bytes")
            result = process_duckdb_file(temp_file_path, cleanup=False)
            logging.info(f"DuckDB processing result: {result}")
            
            if result['status'] == 'error':
                raise HTTPException(
                    status_code=422, 
                    detail=f"Failed to process DuckDB file: {result.get('error', 'Unknown error')}"
                )
            
            # Register file in the registry for analytical queries
            try:
                file_id = file_registry.register_file(
                    filename=file.filename,
                    db_path=result['db_path'],
                    metadata=result
                )
                logging.info(f"Registered file in registry: {file_id}")
            except Exception as e:
                logging.warning(f"Failed to register file in registry: {e}")
            
            # Return successful response
            return DuckDBProcessingResult(**result)
            
        except DuckDBProcessingError as e:
            raise HTTPException(status_code=422, detail=f"DuckDB processing error: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error processing DuckDB file: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error during file processing")
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logging.error(f"Unexpected error in upload endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        # Note: We don't cleanup the temporary file here as it needs to persist
        # for analytical queries. The session manager will handle cleanup when sessions expire.
        pass

@app.post("/api/analyze-duckdb")
async def analyze_duckdb(request: AnalyticalQueryRequest):
    """
    Execute analytical queries against DuckDB databases with clean text responses.
    
    This endpoint provides analytical results with only clean text responses,
    never exposing JSON structure to users.
    
    Args:
        request: AnalyticalQueryRequest containing question, db_path, and optional session_id
        
    Returns:
        JSON response with clean text answer only
        
    Raises:
        HTTPException: For various error conditions
    """
    try:
        # Execute the detailed analytical query
        result = execute_analytical_query_detailed(
            question=request.question,
            db_path=request.db_path,
            session_id=request.session_id
        )
        
        if not result.get('success', False):
            # Return clean error message, not JSON structure
            error_message = result.get('error', 'Analytical query failed')
            return {"response": f"I encountered an error while analyzing your data: {error_message}"}
        
        # Extract and return only the clean answer text
        clean_answer = extract_clean_response(result)
        return {"response": clean_answer}
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logging.error(f"Unexpected error in analyze-duckdb endpoint: {str(e)}")
        # Return clean error message, not technical details
        return {"response": "I encountered an unexpected error while analyzing your data. Please try again or contact support if the issue persists."}

@app.post("/api/chat")
async def handle_chat_data(request: FastAPIRequest, protocol: str = Query('data')):
    try:
        body = await request.json()
        logging.info(f"Request body: {body}")
        
        # Manually validate the request body
        request_data = Request(**body)
        
        messages = request_data.messages
        gemini_messages = convert_to_gemini_messages(messages)

        response = StreamingResponse(stream_text(gemini_messages, protocol))
        response.headers['x-vercel-ai-data-stream'] = 'v1'
        return response
    except ValidationError as e:
        logging.error(f"Validation error: {e.errors()}")
        return JSONResponse(status_code=422, content={"detail": e.errors()})
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})