# -*- coding: utf-8 -*-

from datetime import date, datetime
import unittest

from ezbbg.ws.server import isoformat_date_converter


class ServerUtilityTestCase(unittest.TestCase):
    def test_date_isoformat_converter(self):
        strdate = "1926-09-23"
        expected = date(1926, 9, 23)
        self.assertEqual(expected, isoformat_date_converter(strdate))

    def test_datetime_isoformat_converter(self):
        strdatetime = "1926-05-26T15:23:45.525000"
        expected = datetime(1926, 5, 26, 15, 23, 45, 525000)
        self.assertEqual(expected, isoformat_date_converter(strdatetime))


if __name__ == '__main__':
    unittest.main()
