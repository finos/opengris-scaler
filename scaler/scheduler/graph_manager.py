import asyncio
import dataclasses
import enum
from asyncio import Queue
from typing import Dict, List, Optional, Set, Tuple, Union

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.async_object_storage_connector import AsyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata, TaskStatus
from scaler.protocol.python.message import GraphTask, GraphTaskCancel, StateGraphTask, Task, TaskCancel, TaskResult
from scaler.scheduler.mixins import ClientManager, GraphTaskManager, ObjectManager, TaskManager
from scaler.utility.graph.topological_sorter import TopologicalSorter
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
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
    result_object_ids: List[ObjectID] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class _Graph:
    target_task_ids: List[TaskID]
    sorter: TopologicalSorter
    tasks: Dict[TaskID, _TaskInfo]
    depended_task_id_to_task_id: ManyToManyDict[TaskID, TaskID]
    client: ClientID
    status: _GraphState = dataclasses.field(default=_GraphState.Running)
    running_task_ids: Set[TaskID] = dataclasses.field(default_factory=set)


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
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        self._client_manager: Optional[ClientManager] = None
        self._task_manager: Optional[TaskManager] = None
        self._object_manager: Optional[ObjectManager] = None

        self._unassigned: Queue = Queue()

        self._graph_task_id_to_graph: Dict[TaskID, _Graph] = dict()
        self._task_id_to_graph_task_id: Dict[TaskID, TaskID] = dict()

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        client_manager: ClientManager,
        task_manager: TaskManager,
        object_manager: ObjectManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._connector_storage = connector_storage
        self._client_manager = client_manager
        self._task_manager = task_manager
        self._object_manager = object_manager

    async def on_graph_task(self, client_id: ClientID, graph_task: GraphTask):
        await self._unassigned.put((client_id, graph_task))

    async def on_graph_task_cancel(self, client_id: ClientID, graph_task_cancel: GraphTaskCancel):
        if graph_task_cancel.task_id not in self._graph_task_id_to_graph:
            await self._binder.send(client_id, TaskResult.new_msg(graph_task_cancel.task_id, TaskStatus.NotFound))
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

    def is_graph_sub_task(self, task_id: TaskID):
        return task_id in self._task_id_to_graph_task_id

    async def routine(self):
        client, graph_task = await self._unassigned.get()
        await self.__add_new_graph(client, graph_task)

    def get_status(self) -> Dict:
        return {"graph_manager": {"unassigned": self._unassigned.qsize()}}

    async def __add_new_graph(self, client_id: ClientID, graph_task: GraphTask):
        graph = {}

        self._client_manager.on_task_begin(client_id, graph_task.task_id)

        tasks = dict()
        depended_task_id_to_task_id: ManyToManyDict[TaskID, TaskID] = ManyToManyDict()
        for task in graph_task.graph:
            self._task_id_to_graph_task_id[task.task_id] = graph_task.task_id
            tasks[task.task_id] = _TaskInfo(_NodeTaskState.Inactive, task)

            required_task_ids = {arg for arg in task.function_args if isinstance(arg, TaskID)}
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
            graph_task.targets, sorter, tasks, depended_task_id_to_task_id, client_id
        )
        await self.__check_one_graph(graph_task.task_id)

    async def __check_one_graph(self, graph_task_id: TaskID):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        if not graph_info.sorter.is_active():
            await self.__finish_one_graph(graph_task_id, TaskStatus.Success)
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
                function_args=[self.__get_argument_object(graph_task_id, arg) for arg in task_info.task.function_args],
            )

            await self._task_manager.on_task_new(graph_info.client, task)

    async def __mark_node_done(self, result: TaskResult):
        graph_task_id = self._task_id_to_graph_task_id.pop(result.task_id)

        graph_info = self._graph_task_id_to_graph[graph_task_id]

        task_info = graph_info.tasks[result.task_id]

        task_info.result_object_ids = [ObjectID(object_id_bytes) for object_id_bytes in result.results]

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

        await self.__clean_intermediate_result(graph_task_id, result.task_id)
        graph_info.sorter.done(result.task_id)

        if result.task_id in graph_info.running_task_ids:
            graph_info.running_task_ids.remove(result.task_id)

        if result.task_id in graph_info.target_task_ids:
            await self._binder.send(graph_info.client, result)

    async def __cancel_one_graph(self, graph_task_id: TaskID, result: TaskResult):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        graph_info.status = _GraphState.Canceling

        if not self.__is_graph_finished(graph_task_id):
            result_status = result.status
            result_metadata = result.metadata
            result_object_ids = [ObjectID(object_id_bytes) for object_id_bytes in result.results]
            result_objects = [
                (
                    object_id,
                    self._object_manager.get_object_name(object_id),
                )
                for object_id in result_object_ids
            ]
            await self.__clean_all_running_nodes(graph_task_id, result_status, result_metadata, result_objects)
            await self.__clean_all_inactive_nodes(graph_task_id, result_status, result_metadata, result_objects)

        await self.__finish_one_graph(graph_task_id, result.status)

    async def __clean_all_running_nodes(
        self,
        graph_task_id: TaskID,
        result_status: TaskStatus,
        result_metadata: bytes,
        result_objects: List[Tuple[ObjectID, bytes]],
    ):
        graph_info = self._graph_task_id_to_graph[graph_task_id]

        running_task_ids = graph_info.running_task_ids.copy()

        # cancel all running tasks
        for task_id in running_task_ids:
            new_result_object_ids = await self.__duplicate_objects(graph_info.client, result_objects)
            await self._task_manager.on_task_cancel(graph_info.client, TaskCancel.new_msg(task_id))
            await self.__mark_node_done(
                TaskResult.new_msg(
                    task_id,
                    result_status,
                    result_metadata,
                    [bytes(object_id) for object_id in new_result_object_ids]
                )
            )

    async def __clean_all_inactive_nodes(
        self,
        graph_task_id: TaskID,
        result_status: TaskStatus,
        result_metadata: bytes,
        result_objects: List[Tuple[ObjectID, bytes]],
    ):
        graph_info = self._graph_task_id_to_graph[graph_task_id]

        while graph_info.sorter.is_active():
            ready_task_ids = graph_info.sorter.get_ready()

            for task_id in ready_task_ids:
                new_result_object_ids = await self.__duplicate_objects(graph_info.client, result_objects)
                await self.__mark_node_done(
                    TaskResult.new_msg(
                        task_id,
                        result_status,
                        result_metadata,
                        [bytes(object_id) for object_id in new_result_object_ids],
                    )
                )

    async def __finish_one_graph(self, graph_task_id: TaskID, result_status: TaskStatus):
        self._client_manager.on_task_finish(graph_task_id)
        info = self._graph_task_id_to_graph.pop(graph_task_id)
        await self._binder.send(info.client, TaskResult.new_msg(graph_task_id, result_status, metadata=b""))

    def __is_graph_finished(self, graph_task_id: TaskID):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        return not graph_info.sorter.is_active() and not graph_info.running_task_ids

    def __get_argument_object(self, graph_task_id: TaskID, argument: Union[TaskID, ObjectID]) -> ObjectID:
        if isinstance(argument, ObjectID):
            return argument

        assert isinstance(argument, TaskID)

        graph_info = self._graph_task_id_to_graph[graph_task_id]
        task_info = graph_info.tasks[argument]

        assert len(task_info.result_object_ids) == 1

        return task_info.result_object_ids[0]

    async def __clean_intermediate_result(self, graph_task_id: TaskID, task_id: TaskID):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        task_info = graph_info.tasks[task_id]

        for argument in task_info.task.function_args:
            if not isinstance(argument, TaskID):
                continue

            graph_info.depended_task_id_to_task_id.remove(argument, task_id)
            if graph_info.depended_task_id_to_task_id.has_left_key(argument):
                continue

            if argument in graph_info.target_task_ids:
                continue

            # delete intermediate results as they are not needed anymore
            self._object_manager.on_del_objects(graph_info.client, set(graph_info.tasks[argument].result_object_ids))

    async def __duplicate_objects(
        self,
        owner: ClientID,
        result_objects: List[Tuple[ObjectID, bytes]]
    ) -> List[ObjectID]:
        new_result_object_ids = [ObjectID.generate_object_id(owner) for _ in result_objects]

        futures = [
            self.__duplicate_object(owner, result_object_id, result_object_name, new_object_id)
            for (result_object_id, result_object_name), new_object_id
            in zip(result_objects, new_result_object_ids)
        ]

        await asyncio.gather(*futures)

        return new_result_object_ids

    async def __duplicate_object(
        self,
        owner: ClientID,
        object_id: ObjectID,
        object_name: bytes,
        new_object_id: ObjectID
    ):
        object_payload = await self._connector_storage.get_object(object_id)
        await self._connector_storage.set_object(new_object_id, object_payload)

        self._object_manager.on_add_object(owner, new_object_id, ObjectMetadata.ObjectContentType.Object, object_name)
