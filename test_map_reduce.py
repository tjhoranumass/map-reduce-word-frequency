import unittest
from map_reduce import map_phase, reduce_phase

class TestMapReduce(unittest.TestCase):
    def test_map_phase(self):
        """Test that the map phase correctly counts words in a single file."""
        input_text = "data engineering big data map reduce"
        with open("test_input.txt", "w") as f:
            f.write(input_text)
        expected_output = {"data": 2, "engineering": 1, "big": 1, "map": 1, "reduce": 1}
        result = map_phase("test_input.txt")
        self.assertEqual(result, expected_output)

    def test_reduce_phase(self):
        """Test that the reduce phase correctly aggregates counts."""
        mapped_data = [
            {"data": 2, "engineering": 1},
            {"data": 1, "map": 1, "reduce": 1},
        ]
        expected_output = {"data": 3, "engineering": 1, "map": 1, "reduce": 1}
        result = reduce_phase(mapped_data)
        self.assertEqual(result, expected_output)

if __name__ == "__main__":
    unittest.main()
