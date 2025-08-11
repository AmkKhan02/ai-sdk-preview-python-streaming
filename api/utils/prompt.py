import json
from enum import Enum
from pydantic import BaseModel
import base64
from typing import List, Optional, Any, Dict
from .attachment import ClientAttachment

class ToolInvocationState(str, Enum):
    CALL = 'call'
    PARTIAL_CALL = 'partial-call'
    RESULT = 'result'

class ToolInvocation(BaseModel):
    state: ToolInvocationState
    toolCallId: str
    toolName: str
    args: Any
    result: Any

class ClientMessage(BaseModel):
    role: str
    content: str
    experimental_attachments: Optional[List[ClientAttachment]] = None
    toolInvocations: Optional[List[ToolInvocation]] = None

def convert_to_gemini_messages(messages: List[ClientMessage]) -> Dict:
    gemini_messages = []
    system_instruction = None

    for message in messages:
        # Handle system messages
        if message.role == "system":
            system_instruction = message.content
            continue

        # Convert role mapping
        role = "user" if message.role in ["user", "human"] else "model"
        
        # Build content parts
        parts = []
        
        # Add text content
        if message.content:
            parts.append({
                "text": message.content
            })

        # Handle attachments
        if message.experimental_attachments:
            for attachment in message.experimental_attachments:
                if attachment.contentType.startswith('image'):
                    # For Gemini, we need to handle images differently
                    # This assumes the attachment.url is a base64 data URL or file path
                    parts.append({
                        "inline_data": {
                            "mime_type": attachment.contentType,
                            "data": attachment.url  # This might need additional processing
                        }
                    })
                elif attachment.contentType.startswith('text'):
                    parts.append({
                        "text": f"[Attachment: {attachment.url}]"
                    })

        # Handle tool invocations/function calls
        if message.toolInvocations:
            for tool_invocation in message.toolInvocations:
                if tool_invocation.state == ToolInvocationState.CALL:
                    # This represents a function call from the model
                    parts.append({
                        "function_call": {
                            "name": tool_invocation.toolName,
                            "args": tool_invocation.args
                        }
                    })
                elif tool_invocation.state == ToolInvocationState.RESULT:
                    # This represents a function response
                    parts.append({
                        "function_response": {
                            "name": tool_invocation.toolName,
                            "response": tool_invocation.result
                        }
                    })

        if parts:
            gemini_messages.append({
                "role": role,
                "parts": parts
            })

    result = {"contents": gemini_messages}
    if system_instruction:
        result["system_instruction"] = {"parts": [{"text": system_instruction}]}
    
    return result