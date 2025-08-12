import os
from tavily import TavilyClient

def extract_content(urls: set[str]) -> list[dict]:
    """
    Extracts the full content from a set of URLs using Tavily's Extract API.

    Args:
        urls (set[str]): A set of URLs from which to extract content.

    Returns:
        list[dict]: A list of dictionaries, each containing the 'url' and 'raw_content'
                    for the extracted pages.
    """
    api_key = os.environ.get("TAVILY_API_KEY")
    if not api_key:
        raise ValueError("TAVILY_API_KEY environment variable not set. Please add your Tavily API key to your .env.local file.")

    client = TavilyClient(api_key)
    response = client.extract(urls=list(urls))
    
    return response
