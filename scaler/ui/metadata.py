from typing import Dict, List


class TaskTags:
    _task_tags: Dict[bytes, str] = {}

    def __init__(self):
        pass

    def add_task_tags(self, task_id: bytes, task_tags: List[str]):
        if len(task_tags) == 0:
            tags = "<no tags>"
        else:
            tags = "|".join(sorted(task_tags))

        self._task_tags[task_id] = tags

    def get_task_tags(self, task_id: bytes) -> str:
        return self._task_tags.get(task_id, "<no tags>")

    def pop_task_tags(self, task_id: bytes) -> str:
        return self._task_tags.pop(task_id, "<no tags>")
