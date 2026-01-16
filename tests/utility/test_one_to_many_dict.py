import unittest
from scaler.utility.one_to_many_dict import OneToManyDict


class TestOneToManyDict(unittest.TestCase):
    def setUp(self):
        # Create an instance of OneToManyDict before each test
        self._dict = OneToManyDict()

    def test_add(self):
        # Add a key-value pair and check if the dictionary behaves correctly
        self._dict.add('key1', 'value1')
        self.assertIn('key1', self._dict)
        self.assertIn('value1', self._dict.get_values('key1'))

        # Test adding a new value to the same key
        self._dict.add('key1', 'value2')
        values = self._dict.get_values('key1')
        self.assertIn('value2', values)
        self.assertEqual(len(values), 2)

        # Test adding a value that already exists but to a different key (should raise error)
        with self.assertRaises(ValueError):
            self._dict.add('key2', 'value1')

    def test_has_key(self):
        self._dict.add('key1', 'value1')
        self.assertTrue(self._dict.has_key('key1'))
        self.assertFalse(self._dict.has_key('key2'))

    def test_has_value(self):
        self._dict.add('key1', 'value1')
        self.assertTrue(self._dict.has_value('value1'))
        self.assertFalse(self._dict.has_value('value2'))

    def test_get_key(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key2', 'value2')
        self.assertEqual(self._dict.get_key('value1'), 'key1')
        self.assertEqual(self._dict.get_key('value2'), 'key2')

        # Test if value not found
        with self.assertRaises(ValueError):
            self._dict.get_key('value3')

    def test_get_values(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key1', 'value2')
        self.assertEqual(self._dict.get_values('key1'), {'value1', 'value2'})

        # Test if key not found
        with self.assertRaises(ValueError):
            self._dict.get_values('key2')

    def test_remove_key(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key1', 'value2')

        # Remove key and check if it returns the correct values
        values = self._dict.remove_key('key1')
        self.assertEqual(values, {'value1', 'value2'})
        self.assertFalse(self._dict.has_key('key1'))

        # Check that removed values are no longer in the dict
        with self.assertRaises(ValueError):
            self._dict.get_values('key1')

        with self.assertRaises(ValueError):
            self._dict.get_key('value1')

    def test_remove_value(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key1', 'value2')

        key = self._dict.remove_value('value1')
        self.assertEqual(key, 'key1')

        value = self._dict.get_values('key1')
        self.assertEqual(value, {'value2'})

        # Check that only the correct value was removed
        self.assertIn('value2', self._dict.get_values('key1'))

        # Test removing a value that doesn't exist
        with self.assertRaises(ValueError):
            self._dict.remove_value('value3')

    def test_keys(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key2', 'value2')
        self.assertEqual(set(self._dict.keys()), {'key1', 'key2'})

    def test_values(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key1', 'value2')

        values = self._dict.values()
        # the initial version of one_to_many_dict returns dict_value[{v1, v2}]
        self.assertEqual([{'value1', 'value2'}], values)

    def test_items(self):
        self._dict.add('key1', 'value1')
        self._dict.add('key2', 'value2')

        items = self._dict.items()
        self.assertIn(('key1', {'value1'}), items)
        self.assertIn(('key2', {'value2'}), items)

    def test_contains_operator(self):
        self._dict.add('key1', 'value1')
        self.assertTrue('key1' in self._dict)
        self.assertFalse('key2' in self._dict)

    def test_remove_key_edge_case(self):
        # Edge case for removing key that doesn't exist
        with self.assertRaises(KeyError):
            self._dict.remove_key('non_existent_key')

    def test_remove_value_edge_case(self):
        # Edge case for removing value that doesn't exist
        with self.assertRaises(ValueError):
            self._dict.remove_value('non_existent_value')

    def test_iter_alive(self):
        # Key type is different to trigger underlying map to rehash
        self._dict.add('1', 0)
        count = 0
        for _ in self._dict:
            count += 1
            for i in range(1, 12):
                self._dict.add(i, i)
        self.assertTrue(count == 1)

    def test_iter_invalidated(self):
        # Key type is different to trigger underlying map to rehash
        self._dict.add('1', 0)
        count = 0
        with self.assertRaises(RuntimeError):
            for _ in self._dict:
                count += 1
                # Large number of keys to trigger rehash
                for i in range(1, 200):
                    self._dict.add(i, i)
        self.assertTrue(count == 1)
