import os
from tavily import TavilyClient

def get_links(query: str) -> set[str]:
    """
    Performs a web search using Tavily and returns a set of unique URLs.

    Args:
        query (str): The search query to send to Tavily.

    Returns:
        set[str]: A set containing unique URLs from the search results.
    """
    api_key = os.environ.get("TAVILY_API_KEY")
    if not api_key:
        raise ValueError("TAVILY_API_KEY environment variable not set. Please add your Tavily API key to your .env.local file.")

    client = TavilyClient(api_key)
    response = client.search(query=query)
    
    urls = {result['url'] for result in response['results']}
    return urls
