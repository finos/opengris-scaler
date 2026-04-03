import os
import tempfile
import unittest

from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerAdapter


class TestORBAWSEC2WorkerAdapterLoadRequirementsContent(unittest.TestCase):
    def test_returns_literal_string(self):
        content = ORBAWSEC2WorkerAdapter._load_requirements_content("opengris-scaler>=1.0\nboto3\n")
        self.assertEqual(content, "opengris-scaler>=1.0\nboto3\n")

    def test_reads_valid_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("opengris-scaler\n")
            path = f.name
        try:
            content = ORBAWSEC2WorkerAdapter._load_requirements_content(path)
            self.assertEqual(content, "opengris-scaler\n")
        finally:
            os.unlink(path)

    def test_returns_literal_string_when_path_does_not_exist(self):
        content = ORBAWSEC2WorkerAdapter._load_requirements_content("/nonexistent/requirements.txt")
        self.assertEqual(content, "/nonexistent/requirements.txt")


class TestORBAWSEC2WorkerAdapterValidateRequirements(unittest.TestCase):
    def test_raises_when_opengris_scaler_missing(self):
        requirements = "boto3\nrequests>=2.0\n"
        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_passes_when_opengris_scaler_present(self):
        requirements = "boto3\nopengris-scaler>=1.0\n"
        ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_passes_with_underscore_variant(self):
        requirements = "opengris_scaler\n"
        ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_passes_with_extras(self):
        requirements = "opengris-scaler[orb]\n"
        ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_ignores_comments_and_flags(self):
        requirements = "# opengris-scaler\n-r base.txt\nboto3\n"
        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_passes_with_direct_url(self):
        requirements = "opengris-scaler\nhttps://example.com/mypackage.whl\n"
        ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_raises_on_malformed_line(self):
        requirements = "opengris-scaler\n!!!invalid package!!!\n"
        with self.assertRaises(ValueError, msg="Invalid requirement line"):
            ORBAWSEC2WorkerAdapter._validate_requirements(requirements)

    def test_raises_on_local_path(self):
        requirements = "opengris-scaler\n./local_package\n"
        with self.assertRaises(ValueError, msg="Invalid requirement line"):
            ORBAWSEC2WorkerAdapter._validate_requirements(requirements)
