import os
import unittest
from glob import glob

from scaler.utility.logging.utility import setup_logger
from tests.utility import logging_test_name


class TestExamples(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def tearDown(self) -> None:
        pass

    def test_examples(self):
        basic_examples = glob("examples/*.py")
        prefix = "PYTHONPATH=. python "
        for example in basic_examples:
            assert os.system(prefix + example) == 0
