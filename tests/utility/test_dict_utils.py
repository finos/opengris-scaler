import unittest

from scaler.utility.dict_utils import deep_merge


class TestDeepMerge(unittest.TestCase):
    """Unit tests for the deep_merge utility function."""

    def test_scalar_override_wins(self) -> None:
        result = deep_merge({"a": 1}, {"a": 99})
        self.assertEqual(result["a"], 99)

    def test_nested_dict_merged_recursively(self) -> None:
        base = {"spec": {"restartPolicy": "Never", "hostNetwork": False}}
        result = deep_merge(base, {"spec": {"hostNetwork": True}})
        self.assertEqual(result["spec"]["restartPolicy"], "Never")
        self.assertEqual(result["spec"]["hostNetwork"], True)

    def test_list_replaced_entirely(self) -> None:
        result = deep_merge({"volumes": [{"name": "a"}, {"name": "b"}]}, {"volumes": [{"name": "c"}]})
        self.assertEqual(result["volumes"], [{"name": "c"}])

    def test_empty_override_is_noop(self) -> None:
        base = {"a": 1, "b": {"c": 2}}
        self.assertEqual(deep_merge(base, {}), base)

    def test_new_key_added(self) -> None:
        result = deep_merge({"a": 1}, {"b": 2})
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 2)

    def test_base_is_not_mutated(self) -> None:
        base = {"a": {"x": 1}}
        deep_merge(base, {"a": {"y": 2}})
        self.assertNotIn("y", base["a"])
