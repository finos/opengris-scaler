import unittest
from typing import get_origin

from scaler.utility.many_to_many_dict import ManyToManyDict
from scaler.utility.one_to_many_dict import OneToManyDict
from scaler.utility.queues.indexed_queue import IndexedQueue
from scaler.utility.queues.stable_priority_queue import StablePriorityQueue


class TestGenericExtensionTypes(unittest.TestCase):
    def test_subscript_matches_stub(self) -> None:
        """Each C++ type its stub declares Generic is subscriptable at runtime, as annotations need on Python < 3.14."""
        self.assertIs(get_origin(IndexedQueue[int]), IndexedQueue)
        self.assertIs(get_origin(StablePriorityQueue[int, str]), StablePriorityQueue)
        self.assertIs(get_origin(ManyToManyDict[str, int]), ManyToManyDict)
        self.assertIs(get_origin(OneToManyDict[str, int]), OneToManyDict)

    def test_alias_instantiates_the_type(self) -> None:
        """Calling a subscripted type builds an instance of the type itself."""
        self.assertIsInstance(StablePriorityQueue[int, str](), StablePriorityQueue)
