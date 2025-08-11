import os
import json
import uuid
import logging
from typing import List, Dict, Any  # Added Dict import here
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request as FastAPIRequest
from fastapi.responses import StreamingResponse, JSONResponse
import google.generativeai as genai
from .utils.prompt import ClientMessage, convert_to_gemini_messages
from .utils.tools import get_current_weather, create_graph

load_dotenv(".env.local")

app = FastAPI()

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

class Request(BaseModel):
    messages: List[ClientMessage]

available_tools = {
    "get_current_weather": get_current_weather,
    "create_graph": create_graph,
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
            }
        ]
    }
]
def stream_text(messages_data: Dict, protocol: str = 'data'):
    try:
        logging.info(f"Data sent to Gemini: {messages_data}")
        # Initialize the model
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            tools=gemini_tools,
            system_instruction=messages_data.get("system_instruction")
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
                                    
                                    # Yield tool result
                                    yield f'a:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)},"result":{json.dumps(result)}}}\n'
                                    
                                    # Send function response back to model and get final response
                                    function_response = {
                                        "function_response": {
                                            "name": function_call.name,
                                            "response": result
                                        }
                                    }
                                    
                                    # Break out of the loop to avoid processing more chunks
                                    # that might trigger the same function call again
                                    break
                                            
                                except Exception as e:
                                    error_result = {"error": str(e)}
                                    yield f'a:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)},"result":{json.dumps(error_result)}}}\n'

        # End stream
        yield 'e:{"finishReason":"stop","usage":{"promptTokens":0,"completionTokens":0},"isContinued":false}\n'

    except Exception as e:
        # Handle errors
        yield f'0:{json.dumps(f"Error: {str(e)}")}\n'
        yield 'e:{"finishReason":"error","usage":{"promptTokens":0,"completionTokens":0},"isContinued":false}\n'

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
