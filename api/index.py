import os
import json
import uuid
from typing import List, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
import google.generativeai as genai
from .utils.prompt import ClientMessage, convert_to_gemini_messages
from .utils.tools import get_current_weather

load_dotenv(".env.local")

app = FastAPI()

# Configure Gemini
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

class Request(BaseModel):
    messages: List[ClientMessage]

available_tools = {
    "get_current_weather": get_current_weather,
}

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
            }
        ]
    }
]

def stream_text(messages_data: Dict, protocol: str = 'data'):
    try:
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

        for chunk in response:
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
                            tool_call_id = str(uuid.uuid4())
                            
                            # Convert args from struct to dict
                            args = {}
                            if hasattr(function_call, 'args'):
                                for key, value in function_call.args.items():
                                    args[key] = value
                            
                            # Yield tool call
                            yield f'9:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)}}}\n'
                            
                            # Execute the function
                            if function_call.name in available_tools:
                                try:
                                    result = available_tools[function_call.name](**args)
                                    
                                    # Yield tool result
                                    yield f'a:{{"toolCallId":"{tool_call_id}","toolName":"{function_call.name}","args":{json.dumps(args)},"result":{json.dumps(result)}}}\n'
                                    
                                    # Send function response back to model
                                    function_response = {
                                        "function_response": {
                                            "name": function_call.name,
                                            "response": result
                                        }
                                    }
                                    
                                    # Continue conversation with function result
                                    continue_response = chat.send_message(
                                        [function_response],
                                        stream=True
                                    )
                                    
                                    for continue_chunk in continue_response:
                                        if hasattr(continue_chunk, 'text') and continue_chunk.text:
                                            yield f'0:{json.dumps(continue_chunk.text)}\n'
                                            
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
async def handle_chat_data(request: Request, protocol: str = Query('data')):
    messages = request.messages
    gemini_messages = convert_to_gemini_messages(messages)

    response = StreamingResponse(stream_text(gemini_messages, protocol))
    response.headers['x-vercel-ai-data-stream'] = 'v1'
    return response