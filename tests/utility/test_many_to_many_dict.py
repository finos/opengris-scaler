import unittest
from scaler.utility.many_to_many_dict import ManyToManyDict


class TestManyToManyDict(unittest.TestCase):
    def setUp(self):
        self._dict = ManyToManyDict[str, int]()

    def test_add_and_has_key_pair(self):
        self._dict.add("a", 1)

        self.assertTrue(self._dict.has_left_key("a"))
        self.assertTrue(self._dict.has_right_key(1))
        self.assertTrue(self._dict.has_key_pair("a", 1))

    def test_add_multiple_relationships(self):
        self._dict.add("a", 1)
        self._dict.add("a", 2)
        self._dict.add("b", 1)

        self.assertEqual(self._dict.get_right_items("a"), {1, 2})
        self.assertEqual(self._dict.get_left_items(1), {"a", "b"})

    def test_left_and_right_keys(self):
        self._dict.add("a", 1)
        self._dict.add("b", 2)

        self.assertEqual(set(self._dict.left_keys()), {"a", "b"})
        self.assertEqual(set(self._dict.right_keys()), {1, 2})

    def test_remove_key_pair(self):
        self._dict.add("a", 1)
        self._dict.add("a", 2)

        self._dict.remove("a", 1)

        self.assertFalse(self._dict.has_key_pair("a", 1))
        self.assertTrue(self._dict.has_key_pair("a", 2))
        self.assertEqual(self._dict.get_right_items("a"), {2})

    def test_remove_last_value_removes_key(self):
        self._dict.add("a", 1)
        self._dict.remove("a", 1)

        self.assertFalse(self._dict.has_left_key("a"))
        self.assertFalse(self._dict.has_right_key(1))

    def test_remove_left_key(self):
        self._dict.add("a", 1)
        self._dict.add("a", 2)
        self._dict.add("b", 2)

        removed = self._dict.remove_left_key("a")

        self.assertEqual(removed, {1, 2})
        self.assertFalse(self._dict.has_left_key("a"))
        self.assertEqual(self._dict.get_left_items(2), {"b"})

    def test_remove_right_key(self):
        self._dict.add("a", 1)
        self._dict.add("b", 1)

        removed = self._dict.remove_right_key(1)

        self.assertEqual(removed, {"a", "b"})
        self.assertFalse(self._dict.has_right_key(1))
        self.assertFalse(self._dict.has_left_key("a"))
        self.assertFalse(self._dict.has_left_key("b"))

    def test_get_right_items_missing_left_key(self):
        with self.assertRaises(ValueError):
            self._dict.get_right_items("missing")

    def test_get_left_items_missing_right_key(self):
        with self.assertRaises(ValueError):
            self._dict.get_left_items(999)

    def test_remove_left_key_missing(self):
        with self.assertRaises(KeyError):
            self._dict.remove_left_key("missing")

    def test_remove_right_key_missing(self):
        with self.assertRaises(KeyError):
            self._dict.remove_right_key(999)

    def test_items_views(self):
        self._dict.add("a", 1)
        self._dict.add("a", 2)

        left_items = dict(self._dict.left_key_items())
        right_items = dict(self._dict.right_key_items())

        self.assertEqual(left_items, {"a": {1, 2}})
        self.assertEqual(right_items, {1: {"a"}, 2: {"a"}})
