import json
from enum import Enum
from pydantic import BaseModel
import base64
from typing import List, Optional, Any, Dict
import google.generativeai as genai

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
    result: Optional[Any] = None

class ClientMessage(BaseModel):
    role: str
    content: str
    experimental_attachments: Optional[List['ClientAttachment']] = None
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
                else:
                    try:
                        # Decode the file content from base64
                        base64_data = attachment.url.split(',')[1]
                        file_data = base64.b64decode(base64_data).decode('utf-8')
                        
                        # Include the file content directly in the message
                        parts.append({
                            "text": f"The user has attached the following file: {attachment.name}\n\n---\n{file_data}\n---"
                        })
                        
                    except Exception as e:
                        parts.append({
                            "text": f"[Attachment: {attachment.name} of type {attachment.contentType} could not be processed. Error: {e}]"
                        })

        # IMPORTANT: Exclude tool invocations/function calls from message history
        # 
        # The previous implementation included toolInvocations in the message history,
        # which caused function call/response mismatches in subsequent messages.
        # The session context (like database state) is maintained by the session manager,
        # not by function call history. Function calls should only be processed in real-time,
        # not replayed from history.
        #
        # This fixes the Error 400: "function response parts mismatch" issue.

        if parts:
            gemini_messages.append({
                "role": role,
                "parts": parts
            })

    result = {"contents": gemini_messages}
    if system_instruction:
        result["system_instruction"] = {"parts": [{"text": system_instruction}]}
    
    return result
