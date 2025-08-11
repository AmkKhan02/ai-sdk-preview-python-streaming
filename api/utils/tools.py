import requests
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64

def get_current_weather(latitude, longitude):
    # Format the URL with proper parameter substitution
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m&hourly=temperature_2m&daily=sunrise,sunset&timezone=auto"

    try:
        # Make the API call
        response = requests.get(url)

        # Raise an exception for bad status codes
        response.raise_for_status()

        # Return the JSON response
        return response.json()

    except requests.RequestException as e:
        # Handle any errors that occur during the request
        print(f"Error fetching weather data: {e}")
        return None

def create_graph(data: list, graph_type: str, title: str = "Graph", x_label: str = "X-axis", y_label: str = "Y-axis"):
    """
    Generates a graph from the provided data.
    """
    try:
        # Ensure data is in the correct format (list of dictionaries)
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            try:
                # Attempt to convert if it's a different but compatible format
                data = [dict(item) for item in data]
            except (TypeError, ValueError):
                return {"error": "Invalid data format. Expected a list of dictionaries."}

        df = pd.DataFrame(data)
        
        if df.empty:
            return {"error": "Data is empty"}

        # Set up the plot
        plt.figure(figsize=(10, 6))
        
        # Determine x and y columns
        columns = df.columns.tolist()
        
        if len(columns) < 2:
            return {"error": "Data must have at least 2 columns"}
        
        # Try to identify x and y columns intelligently
        x_col = None
        y_col = None
        
        for col in columns:
            if df[col].dtype in ['object', 'string'] or col.lower() in ['label', 'name', 'category', 'month']:
                x_col = col
            elif pd.api.types.is_numeric_dtype(df[col]) and col.lower() in ['value', 'amount', 'count', 'price']:
                y_col = col
        
        # Fallback to first two columns if not found
        if x_col is None:
            x_col = columns[0]
        if y_col is None:
            # Find first numeric column
            for col in columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    y_col = col
                    break
            if y_col is None:
                y_col = columns[1]  # Fallback to second column
        
        # Create the plot
        if graph_type == 'bar':
            plt.bar(df[x_col], df[y_col])
            plt.xticks(rotation=45, ha='right')
        elif graph_type == 'line':
            plt.plot(df[x_col], df[y_col], marker='o')
            plt.xticks(rotation=45, ha='right')
        else:
            return {"error": f"Unsupported graph type: {graph_type}. Supported types: 'bar', 'line'"}

        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.tight_layout()
        
        # Save to base64
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        
        image_base64 = base64.b64encode(buf.read()).decode('utf-8')
        
        plt.close()  # Important: close the figure to free memory
        
        return {"image": image_base64}

    except Exception as e:
        plt.close()  # Make sure to close the figure even if there's an error
        return {"error": f"Error creating graph: {str(e)}"}