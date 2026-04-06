import unittest

from scaler.config.section.oci_raw_worker_adapter import OCIRawWorkerAdapterConfig


class OCIRawWorkerAdapterConfigTest(unittest.TestCase):
    def test_scaler_package_field_defaults_and_env_mapping(self) -> None:
        scaler_package_field = OCIRawWorkerAdapterConfig.__dataclass_fields__["scaler_package"]

        self.assertEqual(scaler_package_field.default, "opengris-scaler[oci]")
        self.assertEqual(scaler_package_field.metadata["env_var"], "SCALER_PACKAGE")


if __name__ == "__main__":
    unittest.main()
