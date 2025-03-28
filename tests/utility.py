import logging
import unittest


def logging_test_name(obj: unittest.TestCase):
    logging.info(f"{obj.__class__.__name__}:{obj._testMethodName} ==============================================")
