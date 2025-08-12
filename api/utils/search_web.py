import json
from .get_links import get_links
from .extract_content import extract_content

def search_web(query: str) -> str:
    """
    Searches the web for a given query, extracts content from the resulting links,
    and returns a JSON object with URLs and their content.

    Args:
        query (str): The search query.

    Returns:
        str: A JSON string representing a list of dictionaries, where each
             dictionary contains a 'url' and its 'raw_content'.
    """
    links = get_links(query)
    content_list = extract_content(links)
    return json.dumps(content_list, indent=4)

