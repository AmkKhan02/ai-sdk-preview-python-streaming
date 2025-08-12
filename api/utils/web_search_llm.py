import os
import google.generativeai as genai
import logging
from .search_web import search_web

# Configure Gemini (it should already be configured in the main module, but just to be safe)
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

def generate_web_search_query(user_question: str) -> str:
    """
    Use LLM to generate an optimal web search query from a user's question.
    
    Args:
        user_question (str): The user's original question
        
    Returns:
        str: A refined search query optimized for web search
    """
    try:
        model = genai.GenerativeModel("gemini-2.5-flash-lite")
        
        prompt = f"""
You are a search query optimization expert. Given a user's question, generate a concise and effective web search query that would yield the most relevant and up-to-date information.

Rules:
1. Extract the key concepts and intent from the user's question
2. Remove conversational elements like "Can you tell me", "I want to know", etc.
3. Use specific, searchable terms
4. Include relevant keywords that would help find authoritative sources
5. Keep it concise (2-8 words typically)
6. For current events, include terms like "latest", "2024", "recent", "news"

User Question: {user_question}

Return ONLY the optimized search query, nothing else:
"""
        
        response = model.generate_content(prompt)
        search_query = response.text.strip()
        logging.info(f"Generated search query: '{search_query}' for user question: '{user_question}'")
        return search_query
        
    except Exception as e:
        logging.error(f"Error generating search query: {str(e)}")
        # Fallback: return the original question
        return user_question

def answer_web_search_question(query: str, web_search_results: str) -> str:
    """
    Use LLM to generate a comprehensive answer based on web search results.
    
    Args:
        query (str): The original user question
        web_search_results (str): JSON string containing web search results
        
    Returns:
        str: A clean, comprehensive answer based on the web search results
    """
    try:
        model = genai.GenerativeModel("gemini-2.5-flash-lite")
        
        prompt = f"""
You are an AI assistant that provides comprehensive answers based on web search results. 

User's Question: {query}

Web Search Results: {web_search_results}

Instructions:
1. Analyze the web search results to find relevant information that answers the user's question
2. Synthesize information from multiple sources when available
3. Provide a clear, well-structured answer that directly addresses the user's question
4. Include specific facts, data, or quotes when relevant
5. If the search results don't contain enough information to fully answer the question, be honest about the limitations
6. Maintain a helpful and informative tone
7. DO NOT mention that you're using web search results - just provide the answer naturally
8. If there are conflicting information from different sources, acknowledge this and present multiple perspectives

Provide a comprehensive answer based on the search results:
"""
        
        response = model.generate_content(prompt)
        answer = response.text.strip()
        logging.info(f"Generated web search answer for query: '{query}'")
        return answer
        
    except Exception as e:
        logging.error(f"Error generating web search answer: {str(e)}")
        return f"I encountered an error while processing the web search results for your question: {query}. Please try again."

