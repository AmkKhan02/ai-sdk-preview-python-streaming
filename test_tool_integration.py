import unittest
from unittest.mock import MagicMock, patch, call
import json
from api.index import stream_text

class TestToolIntegration(unittest.TestCase):

    @patch('api.index.genai.GenerativeModel')
    def test_multiple_function_calls_are_consolidated(self, mock_generative_model):
        """
        Tests if multiple function call responses are consolidated into a single message.
        """
        # Mock the chat object and its send_message method
        mock_chat = MagicMock()
        mock_generative_model.return_value.start_chat.return_value = mock_chat

        # Mock the response from the first send_message call to include two function calls
        mock_response_chunk_1 = MagicMock()
        mock_response_chunk_1.candidates = [MagicMock()]
        mock_response_chunk_1.candidates[0].content.parts = [
            MagicMock(function_call=MagicMock(name='get_current_weather', args={'latitude': 34.05, 'longitude': -118.25}))
        ]
        mock_response_chunk_1.text = None

        mock_response_chunk_2 = MagicMock()
        mock_response_chunk_2.candidates = [MagicMock()]
        mock_response_chunk_2.candidates[0].content.parts = [
            MagicMock(function_call=MagicMock(name='list_available_databases', args={}))
        ]
        mock_response_chunk_2.text = None
        
        # Mock the final text response after sending consolidated function responses
        mock_final_response_chunk = MagicMock()
        mock_final_response_chunk.text = "Final consolidated response."
        mock_final_response_chunk.candidates = None

        # Set up the return values for send_message
        mock_chat.send_message.side_effect = [
            [mock_response_chunk_1, mock_response_chunk_2],  # First call returns two function calls
            [mock_final_response_chunk]  # Second call returns the final text response
        ]

        # The messages data to be sent to stream_text
        messages_data = {
            "contents": [{"parts": [{"text": "What's the weather in LA and what databases are available?"}]}]
        }

        # Run the stream_text function
        generator = stream_text(messages_data)
        
        # Consume the generator to execute the logic
        output = list(generator)

        # Assert that send_message was called twice
        self.assertEqual(mock_chat.send_message.call_count, 2, "send_message should be called twice")

        # Assert the contents of the second call to send_message
        second_call_args = mock_chat.send_message.call_args_list[1]
        consolidated_response = second_call_args[0][0] # First argument of the second call
        
        # Check that the consolidated response has the correct structure
        self.assertIn("parts", consolidated_response)
        self.assertEqual(len(consolidated_response["parts"]), 2, "Should have two function responses in parts")
        
        # Check the details of the consolidated function responses
        self.assertEqual(consolidated_response["parts"][0]["function_response"]["name"], "get_current_weather")
        self.assertEqual(consolidated_response["parts"][1]["function_response"]["name"], "list_available_databases")

if __name__ == '__main__':
    unittest.main()
