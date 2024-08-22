import dataclasses
import enum
import uuid
from asyncio import Queue
from typing import Dict, List, Optional, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import GraphTask, GraphTaskCancel, StateGraphTask, Task, TaskCancel, TaskResult
from scaler.scheduler.mixins import ClientManager, GraphTaskManager, ObjectManager, TaskManager
from scaler.utility.graph.topological_sorter import TopologicalSorter
from scaler.utility.many_to_many_dict import ManyToManyDict
from scaler.utility.mixins import Looper, Reporter


class _NodeTaskState(enum.Enum):
    Inactive = enum.auto()
    Running = enum.auto()
    Canceled = enum.auto()
    Failed = enum.auto()
    Success = enum.auto()


class _GraphState(enum.Enum):
    Running = enum.auto()
    Canceling = enum.auto()


@dataclasses.dataclass
class _TaskInfo:
    state: _NodeTaskState
    task: Task
    result_object_ids: List[bytes] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class _Graph:
    target_task_ids: List[bytes]
    sorter: TopologicalSorter
    tasks: Dict[bytes, _TaskInfo]
    depended_task_id_to_task_id: ManyToManyDict[bytes, bytes]
    client: bytes
    status: _GraphState = dataclasses.field(default=_GraphState.Running)
    running_task_ids: Set[bytes] = dataclasses.field(default_factory=set)


class VanillaGraphTaskManager(GraphTaskManager, Looper, Reporter):
    """
    A = func()
    B = func2(A)
    C = func3(A)
    D = func4(B, C)

    graph
    A = Task(func)
    B = Task(func2, A)
    C = Task(func3, A)
    D = Task(func4, B, C)

    dependencies
    {"A": {B, C}
     "B": {D},
     "C": {D},
     "D": {},
    }
    """

    def __init__(self):
        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None
        self._client_manager: Optional[ClientManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._object_manager: Optional[ObjectManager] = None

        self._unassigned: Queue = Queue()

        self._graph_task_id_to_graph: Dict[bytes, _Graph] = dict()
        self._task_id_to_graph_task_id: Dict[bytes, bytes] = dict()

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_manager: ClientManager,
        task_manager: TaskManager,
        object_manager: ObjectManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._client_manager = client_manager
        self._task_manager = task_manager
        self._object_manager = object_manager

    async def on_graph_task(self, client: bytes, graph_task: GraphTask):
        await self._unassigned.put((client, graph_task))

    async def on_graph_task_cancel(self, client: bytes, graph_task_cancel: GraphTaskCancel):
        if graph_task_cancel.task_id not in self._graph_task_id_to_graph:
            await self._binder.send(client, TaskResult.new_msg(graph_task_cancel.task_id, TaskStatus.NotFound))
            return

        graph_task_id = self._task_id_to_graph_task_id[graph_task_cancel.task_id]
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        if graph_info.status == _GraphState.Canceling:
            return

        await self.__cancel_one_graph(graph_task_id, TaskResult.new_msg(graph_task_cancel.task_id, TaskStatus.Canceled))

    async def on_graph_sub_task_done(self, result: TaskResult):
        graph_task_id = self._task_id_to_graph_task_id[result.task_id]
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        if graph_info.status == _GraphState.Canceling:
            return

        await self.__mark_node_done(result)

        if result.status == TaskStatus.Success:
            await self.__check_one_graph(graph_task_id)
            return

        assert result.status != TaskStatus.Success
        await self.__cancel_one_graph(graph_task_id, result)

    def is_graph_sub_task(self, task_id: bytes):
        return task_id in self._task_id_to_graph_task_id

    async def routine(self):
        client, graph_task = await self._unassigned.get()
        await self.__add_new_graph(client, graph_task)

    def get_status(self) -> Dict:
        return {"graph_manager": {"unassigned": self._unassigned.qsize()}}

    async def __add_new_graph(self, client: bytes, graph_task: GraphTask):
        graph = {}

        self._client_manager.on_task_begin(client, graph_task.task_id)

        tasks = dict()
        depended_task_id_to_task_id: ManyToManyDict[bytes, bytes] = ManyToManyDict()
        for task in graph_task.graph:
            self._task_id_to_graph_task_id[task.task_id] = graph_task.task_id
            tasks[task.task_id] = _TaskInfo(_NodeTaskState.Inactive, task)

            required_task_ids = {arg.data for arg in task.function_args if arg.type == Task.Argument.ArgumentType.Task}
            for required_task_id in required_task_ids:
                depended_task_id_to_task_id.add(required_task_id, task.task_id)

            graph[task.task_id] = required_task_ids

            await self._binder_monitor.send(
                StateGraphTask.new_msg(
                    graph_task.task_id,
                    task.task_id,
                    (
                        StateGraphTask.NodeTaskType.Target
                        if task.task_id in graph_task.targets
                        else StateGraphTask.NodeTaskType.Normal
                    ),
                    required_task_ids,
                )
            )

        sorter = TopologicalSorter(graph)
        sorter.prepare()

        self._graph_task_id_to_graph[graph_task.task_id] = _Graph(
            graph_task.targets, sorter, tasks, depended_task_id_to_task_id, client
        )
        await self.__check_one_graph(graph_task.task_id)

    async def __check_one_graph(self, graph_task_id: bytes):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        if not graph_info.sorter.is_active():
            await self.__finish_one_graph(graph_task_id, TaskResult.new_msg(graph_task_id, TaskStatus.Success))
            return

        ready_task_ids = graph_info.sorter.get_ready()
        if not ready_task_ids:
            return

        for task_id in ready_task_ids:
            task_info = graph_info.tasks[task_id]
            task_info.state = _NodeTaskState.Running
            graph_info.running_task_ids.add(task_id)

            task = Task.new_msg(
                task_id=task_info.task.task_id,
                source=task_info.task.source,
                metadata=task_info.task.metadata,
                func_object_id=task_info.task.func_object_id,
                function_args=[self.__get_argument(graph_task_id, arg) for arg in task_info.task.function_args],
            )

            await self._task_manager.on_task_new(graph_info.client, task)

    async def __mark_node_done(self, result: TaskResult):
        graph_task_id = self._task_id_to_graph_task_id.pop(result.task_id)

        graph_info = self._graph_task_id_to_graph[graph_task_id]

        task_info = graph_info.tasks[result.task_id]

        task_info.result_object_ids = result.results

        if result.status == TaskStatus.Success:
            task_info.state = _NodeTaskState.Success
        elif result.status == TaskStatus.Canceled:
            task_info.state = _NodeTaskState.Canceled
        elif result.status == TaskStatus.Failed:
            task_info.state = _NodeTaskState.Failed
        elif result.status == TaskStatus.NotFound:
            task_info.state = _NodeTaskState.Canceled

        else:
            raise ValueError(f"received unexpected task result {result}")

        self.__clean_intermediate_result(graph_task_id, result.task_id)
        graph_info.sorter.done(result.task_id)

        if result.task_id in graph_info.running_task_ids:
            graph_info.running_task_ids.remove(result.task_id)

        if result.task_id in graph_info.target_task_ids:
            await self._binder.send(graph_info.client, result)

    async def __cancel_one_graph(self, graph_task_id: bytes, result: TaskResult):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        graph_info.status = _GraphState.Canceling

        if not self.__is_graph_finished(graph_task_id):
            await self.__clean_all_running_nodes(graph_task_id, result)
            await self.__clean_all_inactive_nodes(graph_task_id, result)

        await self.__finish_one_graph(
            graph_task_id, TaskResult.new_msg(result.task_id, result.status, result.metadata, result.results)
        )

    async def __clean_all_running_nodes(self, graph_task_id: bytes, result: TaskResult):
        graph_info = self._graph_task_id_to_graph[graph_task_id]

        running_task_ids = graph_info.running_task_ids.copy()

        # cancel all running tasks
        for task_id in running_task_ids:
            new_result_object_ids = []
            for result_object_id in result.results:
                new_result_object_id = uuid.uuid4().bytes
                self._object_manager.on_add_object(
                    graph_info.client,
                    new_result_object_id,
                    self._object_manager.get_object_name(result_object_id),
                    self._object_manager.get_object_content(result_object_id),
                )
                new_result_object_ids.append(new_result_object_id)

            await self._task_manager.on_task_cancel(graph_info.client, TaskCancel.new_msg(task_id))
            await self.__mark_node_done(
                TaskResult.new_msg(task_id, result.status, result.metadata, new_result_object_ids)
            )

    async def __clean_all_inactive_nodes(self, graph_task_id: bytes, result: TaskResult):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        while graph_info.sorter.is_active():
            ready_task_ids = graph_info.sorter.get_ready()
            for task_id in ready_task_ids:
                new_result_object_ids = []
                for result_object_id in result.results:
                    new_result_object_id = uuid.uuid4().bytes
                    self._object_manager.on_add_object(
                        graph_info.client,
                        new_result_object_id,
                        self._object_manager.get_object_name(result_object_id),
                        self._object_manager.get_object_content(result_object_id),
                    )
                    new_result_object_ids.append(new_result_object_id)

                await self.__mark_node_done(
                    TaskResult.new_msg(task_id, result.status, result.metadata, new_result_object_ids)
                )

    async def __finish_one_graph(self, graph_task_id: bytes, result: TaskResult):
        self._client_manager.on_task_finish(graph_task_id)
        info = self._graph_task_id_to_graph.pop(graph_task_id)
        await self._binder.send(info.client, TaskResult.new_msg(graph_task_id, result.status, results=result.results))

    def __is_graph_finished(self, graph_task_id: bytes):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        return not graph_info.sorter.is_active() and not graph_info.running_task_ids

    def __get_target_results_ids(self, graph_task_id: bytes) -> List[bytes]:
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        return [
            result_object_id
            for task_id in graph_info.target_task_ids
            for result_object_id in graph_info.tasks[task_id].result_object_ids
        ]

    def __get_argument(self, graph_task_id: bytes, argument: Task.Argument) -> Task.Argument:
        if argument.type == Task.Argument.ArgumentType.ObjectID:
            return argument

        assert argument.type == Task.Argument.ArgumentType.Task
        argument_task_id = argument.data

        graph_info = self._graph_task_id_to_graph[graph_task_id]
        task_info = graph_info.tasks[argument_task_id]

        assert len(task_info.result_object_ids) == 1

        return Task.Argument(Task.Argument.ArgumentType.ObjectID, task_info.result_object_ids[0])

    def __clean_intermediate_result(self, graph_task_id: bytes, task_id: bytes):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        task_info = graph_info.tasks[task_id]

        for argument in filter(lambda arg: arg.type == Task.Argument.ArgumentType.Task, task_info.task.function_args):
            argument_task_id = argument.data
            graph_info.depended_task_id_to_task_id.remove(argument_task_id, task_id)
            if graph_info.depended_task_id_to_task_id.has_left_key(argument_task_id):
                continue

            # delete intermediate results as they are not needed anymore
            self._object_manager.on_del_objects(
                graph_info.client, set(graph_info.tasks[argument_task_id].result_object_ids)
            )
