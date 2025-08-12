import unittest
import json
from api.utils.process_duckdb import extract_clean_response

class TestExtractCleanResponse(unittest.TestCase):

    def test_extract_from_valid_dict(self):
        """Test extraction from a dictionary with an 'answer' key."""
        response_data = {"success": True, "answer": "This is the clean answer."}
        self.assertEqual(extract_clean_response(response_data), "This is the clean answer.")

    def test_extract_from_dict_without_answer(self):
        """Test extraction from a dictionary missing the 'answer' key."""
        response_data = {"success": True, "data": "Some data"}
        self.assertEqual(extract_clean_response(response_data), str(response_data))

    def test_extract_from_valid_json_string(self):
        """Test extraction from a JSON string containing an 'answer' key."""
        response_data = json.dumps({"success": True, "answer": "JSON string answer."})
        self.assertEqual(extract_clean_response(response_data), "JSON string answer.")

    def test_extract_from_json_string_without_answer(self):
        """Test extraction from a JSON string missing the 'answer' key."""
        response_data = json.dumps({"success": True, "data": "Some data"})
        self.assertEqual(extract_clean_response(response_data), response_data)

    def test_extract_from_non_json_string(self):
        """Test extraction from a plain text string."""
        response_data = "This is just a plain string."
        self.assertEqual(extract_clean_response(response_data), "This is just a plain string.")

    def test_extract_from_malformed_json_string(self):
        """Test extraction from a malformed JSON string."""
        response_data = '{"success": True, "answer": "Malformed'
        self.assertEqual(extract_clean_response(response_data), response_data)

    def test_extract_from_list(self):
        """Test extraction from a list."""
        response_data = ["item1", "item2"]
        self.assertEqual(extract_clean_response(response_data), str(response_data))

    def test_extract_from_empty_dict(self):
        """Test extraction from an empty dictionary."""
        response_data = {}
        self.assertEqual(extract_clean_response(response_data), str(response_data))

    def test_extract_from_none(self):
        """Test extraction from None."""
        response_data = None
        self.assertEqual(extract_clean_response(response_data), str(response_data))

if __name__ == '__main__':
    unittest.main()
