import unittest
from app.jobs import etl_pipeline
import random
from uuid import uuid1
from datetime import datetime


class test_timeuuid(unittest.TestCase):
    
    def test_valid_timeuuid(self):
        uuid_str = str(uuid1())
        result = etl_pipeline.timeuuid_to_datetime.func(uuid_str)
        self.assertIsNotNone(result)
    
    def test_invalid_timeuuid(self):
        result = etl_pipeline.timeuuid_to_datetime.func("fnafvbr")
        self.assertIsNone(result)
    
    def test_none_timeuuid(self):
        result = etl_pipeline.timeuuid_to_datetime.func(None)
        self.assertIsNone(result)

class test_get_mysql_lastestime(unittest.TestCase):
    def test_none_get_mysql_lastestime(self):
        mysql_time = None
        if mysql_time is None:
            result = datetime.strptime('2022-07-26 15:58:36', '%Y-%m-%d %H:%M:%S')
        else:
            result = None
        self.assertEqual(result,datetime(2022,7,26,15,58,36))

if __name__ == "__main__":
    unittest.main()
