import unittest
from app.jobs import faking_data
import random

class test_fake_data(unittest.TestCase):
    
    def test_event_decide_valid(self):
        result = faking_data.decide_event_type ("HIGH","HOT")

        self.assertIn(
            result, ["click", "conversion", "qualified", "unqualified"]
        )

    def test_bid_positive(self):
        for _ in range(100):
            bid = random.randint(1,20)
            self.assertGreater(bid,0)

if __name__ == "__name__":
    unittest.main()