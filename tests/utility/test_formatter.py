import unittest

from scaler.utility.formatter import format_microseconds


class TestFormatMicroseconds(unittest.TestCase):
    def test_each_unit(self) -> None:
        self.assertEqual(format_microseconds(500), "0.5ms")
        self.assertEqual(format_microseconds(2_500), "2ms")
        self.assertEqual(format_microseconds(999_999_999), "999s")

    def test_a_thousand_seconds_or_more_stays_in_seconds(self) -> None:
        self.assertEqual(format_microseconds(1_000_000_000), "1000s")
        self.assertEqual(format_microseconds(5_000_000_000), "5000+s")
