import asyncio
import dataclasses
import enum
from asyncio import Queue
from typing import Dict, List, Optional, Set, Tuple, Union

from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher
from scaler.protocol.capnp import (
    GraphTask,
    ObjectMetadata,
    StateGraphTask,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
)
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import ClientController, GraphTaskController, ObjectController, TaskController
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
    Aborting = enum.auto()


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


class VanillaGraphTaskController(GraphTaskController, Looper, Reporter):
    """
    Graph Task Manager is on top of normal task manager and will maintain a fake graph task once received graph task,
    In the end, will echo back to client for the graph task
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

    def __init__(self, config_controller: VanillaConfigController):
        self._config_controller = config_controller

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        self._client_controller: Optional[ClientController] = None
        self._task_controller: Optional[TaskController] = None
        self._object_controller: Optional[ObjectController] = None

        self._unassigned: Queue = Queue()

        self._graph_task_id_to_graph: Dict[TaskID, _Graph] = dict()
        self._task_id_to_graph_task_id: Dict[TaskID, TaskID] = dict()

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncPublisher,
        connector_storage: AsyncObjectStorageConnector,
        client_controller: ClientController,
        task_controller: TaskController,
        object_controller: ObjectController,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._connector_storage = connector_storage
        self._client_controller = client_controller
        self._task_controller = task_controller
        self._object_controller = object_controller

    async def on_graph_task(self, client_id: ClientID, graph_task: GraphTask):
        await self._unassigned.put((client_id, graph_task))

    async def on_graph_task_cancel(self, task_cancel: TaskCancel):
        graph_task_id = self._task_id_to_graph_task_id[task_cancel.taskId]

        # received any subtask canceling will lead the whole graph canceling
        await self.__cancel_whole_graph(graph_task_id)

    async def on_graph_sub_task_result(self, result: TaskResult):
        graph_task_id = self._task_id_to_graph_task_id[result.taskId]
        graph_info = self._graph_task_id_to_graph[graph_task_id]

        if graph_info.status == _GraphState.Canceling:
            # there will be case when we are canceling the whole graph, and at the moment, result is returning
            # before we see cancel confirm, we treat them as cancel confirm
            await self.on_graph_sub_task_cancel_confirm(
                TaskCancelConfirm(taskId=result.taskId, cancelConfirmType=TaskCancelConfirmType.canceled)
            )
            return

        self.__mark_node_done(result)

        if result.resultType == TaskResultType.success:
            await self.__check_one_graph(graph_task_id)
            return

        assert result.resultType != TaskResultType.success
        await self.__abort_whole_graph(graph_task_id, result)

    async def on_graph_sub_task_cancel_confirm(self, task_cancel_confirm: TaskCancelConfirm):
        graph_task_id = self._task_id_to_graph_task_id[task_cancel_confirm.taskId]
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        self.__mark_node_canceled(graph_info, task_cancel_confirm)

        await self.__cancel_whole_graph(graph_task_id)

    def is_graph_subtask(self, task_id: TaskID):
        return task_id in self._task_id_to_graph_task_id

    async def routine(self):
        client, graph_task = await self._unassigned.get()
        await self.__add_new_graph(client, graph_task)

    def get_status(self) -> Dict:
        return {"graph_manager": {"unassigned": self._unassigned.qsize()}}

    async def __add_new_graph(self, client_id: ClientID, graph_task: GraphTask):
        graph = {}

        self._client_controller.on_task_begin(client_id, graph_task.taskId)

        # add graph umbrella task, note that umbrella is also a graph subtask
        self._task_id_to_graph_task_id[graph_task.taskId] = graph_task.taskId

        tasks = dict()
        depended_task_id_to_task_id: ManyToManyDict[TaskID, TaskID] = ManyToManyDict()
        for task in graph_task.graph:
            self._task_id_to_graph_task_id[task.taskId] = graph_task.taskId
            tasks[task.taskId] = _TaskInfo(_NodeTaskState.Inactive, task)

            required_task_ids = {
                TaskID(argument.data)
                for argument in task.functionArgs
                if argument.type == Task.Argument.ArgumentType.task
            }
            for required_task_id in required_task_ids:
                depended_task_id_to_task_id.add(required_task_id, task.taskId)

            graph[task.taskId] = required_task_ids

            await self._binder_monitor.send(
                StateGraphTask(
                    graphTaskId=graph_task.taskId,
                    taskId=task.taskId,
                    nodeTaskType=(
                        StateGraphTask.NodeTaskType.target
                        if task.taskId in graph_task.targets
                        else StateGraphTask.NodeTaskType.normal
                    ),
                    parentTaskIds=required_task_ids,
                )
            )

        sorter = TopologicalSorter(graph)
        sorter.prepare()

        self._graph_task_id_to_graph[graph_task.taskId] = _Graph(
            graph_task.targets, sorter, tasks, depended_task_id_to_task_id, client_id
        )
        await self.__check_one_graph(graph_task.taskId)

    async def __check_one_graph(self, graph_task_id: TaskID):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        if not graph_info.sorter.is_active():
            await self.__done_graph_umbrella_task(graph_task_id, TaskResultType.success)
            return

        ready_task_ids = graph_info.sorter.get_ready()
        if not ready_task_ids:
            return

        for task_id in ready_task_ids:
            task_info = graph_info.tasks[task_id]
            task_info.state = _NodeTaskState.Running
            graph_info.running_task_ids.add(task_id)

            task = Task(
                taskId=task_info.task.taskId,
                source=task_info.task.source,
                metadata=task_info.task.metadata,
                funcObjectId=task_info.task.funcObjectId,
                functionArgs=[
                    Task.Argument(
                        type=Task.Argument.ArgumentType.objectID,
                        data=self.__get_argument_object(
                            graph_task_id,
                            (
                                TaskID(argument.data)
                                if argument.type == Task.Argument.ArgumentType.task
                                else ObjectID(argument.data)
                            ),
                        ),
                    )
                    for argument in task_info.task.functionArgs
                ],
                capabilities=capabilities_to_dict(task_info.task.capabilities),
            )

            await self._task_controller.on_task_new(task)

    async def __cancel_whole_graph(self, graph_task_id: TaskID):
        if self.__is_graph_finished(graph_task_id):
            await self.__cancel_graph_umbrella_task(graph_task_id)
            return

        graph_info = self._graph_task_id_to_graph[graph_task_id]

        if graph_info.status in {_GraphState.Canceling, _GraphState.Aborting}:
            # if graph is already in canceling or aborting, we don't need to proceed whole graph canceling again
            return

        graph_info.status = _GraphState.Canceling

        await asyncio.gather(
            *[
                self._task_controller.on_task_cancel(
                    graph_info.client, TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=True))
                )
                for task_id in graph_info.running_task_ids
            ]
        )

        # cancel all inactive tasks
        task_cancel_confirms: List[TaskCancelConfirm] = list()
        while graph_info.sorter.is_active():
            ready_task_ids = graph_info.sorter.get_ready()
            if not ready_task_ids:
                break

            for task_id in ready_task_ids:
                task_cancel_confirm = TaskCancelConfirm(
                    taskId=task_id, cancelConfirmType=TaskCancelConfirmType.canceled
                )
                self.__mark_node_canceled(graph_info, task_cancel_confirm)
                task_cancel_confirms.append(task_cancel_confirm)

        await self.__send_task_cancel_confirms(graph_info.client, task_cancel_confirms)

        if self.__is_graph_finished(graph_task_id):
            await self.__cancel_graph_umbrella_task(graph_task_id)

    @staticmethod
    def __mark_node_canceled(graph_info: _Graph, task_cancel_confirm: TaskCancelConfirm):
        if task_cancel_confirm.taskId not in graph_info.tasks:
            return

        task_info = graph_info.tasks[task_cancel_confirm.taskId]
        if task_cancel_confirm.cancelConfirmType == TaskCancelConfirmType.canceled:
            task_info.state = _NodeTaskState.Canceled
        elif task_cancel_confirm.cancelConfirmType == TaskCancelConfirmType.cancelFailed:
            pass
        elif task_cancel_confirm.cancelConfirmType == TaskCancelConfirmType.cancelNotFound:
            task_info.state = _NodeTaskState.Canceled
        else:
            raise ValueError(f"received unexpected task cancel confirm {task_cancel_confirm}")

        graph_info.sorter.done(task_cancel_confirm.taskId)

        if task_cancel_confirm.taskId in graph_info.running_task_ids:
            graph_info.running_task_ids.remove(task_cancel_confirm.taskId)

    async def __abort_whole_graph(self, graph_task_id: TaskID, result: TaskResult):
        if self.__is_graph_finished(graph_task_id):
            await self.__done_graph_umbrella_task(graph_task_id, result.resultType)
            return

        graph_info = self._graph_task_id_to_graph[graph_task_id]
        graph_info.status = _GraphState.Aborting

        result_object_ids = [ObjectID(object_id_bytes) for object_id_bytes in result.results]
        result_objects = [
            (object_id, self._object_controller.get_object_name(object_id)) for object_id in result_object_ids
        ]

        # mark all running tasks done
        results: List[TaskResult] = list()
        for task_id in graph_info.running_task_ids.copy():
            new_result_object_ids = await self.__duplicate_objects(graph_info.client, result_objects)
            result = TaskResult(
                taskId=task_id,
                resultType=result.resultType,
                metadata=result.metadata,
                results=[bytes(object_id) for object_id in new_result_object_ids],
            )
            self.__mark_node_done(result)
            results.append(result)

        # mark all inactive tasks done
        while graph_info.sorter.is_active():
            for task_id in graph_info.sorter.get_ready():
                new_result_object_ids = await self.__duplicate_objects(graph_info.client, result_objects)
                result = TaskResult(
                    taskId=task_id,
                    resultType=result.resultType,
                    metadata=result.metadata,
                    results=[bytes(object_id) for object_id in new_result_object_ids],
                )
                self.__mark_node_done(result)
                results.append(result)

        await self.__send_results(graph_info.client, results)

        if self.__is_graph_finished(graph_task_id):
            await self.__done_graph_umbrella_task(graph_task_id, result.resultType)
            return

    def __mark_node_done(self, result: TaskResult):
        graph_task_id = self._task_id_to_graph_task_id.pop(result.taskId)
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        task_info = graph_info.tasks[result.taskId]

        task_info.result_object_ids = [ObjectID(object_id_bytes) for object_id_bytes in result.results]

        if result.resultType == TaskResultType.success:
            task_info.state = _NodeTaskState.Success
        elif result.resultType == TaskResultType.failed:
            task_info.state = _NodeTaskState.Failed
        elif result.resultType == TaskResultType.failedWorkerDied:
            task_info.state = _NodeTaskState.Failed
        else:
            raise ValueError(f"received unexpected task result {result}")

        self.__clean_intermediate_result(graph_task_id, result.taskId)
        graph_info.sorter.done(result.taskId)

        if result.taskId in graph_info.running_task_ids:
            graph_info.running_task_ids.remove(result.taskId)

    async def __cancel_graph_umbrella_task(self, graph_task_id: TaskID):
        if not self.__is_graph_finished(graph_task_id):
            return

        self._client_controller.on_task_finish(graph_task_id)
        self._task_id_to_graph_task_id.pop(graph_task_id)
        info = self._graph_task_id_to_graph.pop(graph_task_id)
        await self._binder.send(
            info.client, TaskCancelConfirm(taskId=graph_task_id, cancelConfirmType=TaskCancelConfirmType.canceled)
        )

    async def __done_graph_umbrella_task(self, graph_task_id: TaskID, result_type: TaskResultType):
        self._client_controller.on_task_finish(graph_task_id)
        self._task_id_to_graph_task_id.pop(graph_task_id)
        info = self._graph_task_id_to_graph.pop(graph_task_id)
        await self._binder.send(
            info.client, TaskResult(taskId=graph_task_id, resultType=result_type, metadata=b"", results=[])
        )

    def __is_graph_finished(self, graph_task_id: TaskID):
        if graph_task_id not in self._graph_task_id_to_graph:
            return True

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

    def __clean_intermediate_result(self, graph_task_id: TaskID, task_id: TaskID):
        graph_info = self._graph_task_id_to_graph[graph_task_id]
        task_info = graph_info.tasks[task_id]

        for argument in task_info.task.functionArgs:
            if argument.type != Task.Argument.ArgumentType.task:
                continue

            depended_task_id = TaskID(argument.data)
            graph_info.depended_task_id_to_task_id.remove(depended_task_id, task_id)
            if graph_info.depended_task_id_to_task_id.has_left_key(depended_task_id):
                continue

            if depended_task_id in graph_info.target_task_ids:
                continue

            # delete intermediate results as they are not needed anymore
            self._object_controller.on_del_objects(
                graph_info.client, set(graph_info.tasks[depended_task_id].result_object_ids)
            )

    async def __duplicate_objects(
        self, owner: ClientID, result_objects: List[Tuple[ObjectID, bytes]]
    ) -> List[ObjectID]:
        new_result_object_ids = [ObjectID.generate_object_id(owner) for _ in result_objects]

        futures = [
            self.__duplicate_object(owner, result_object_id, result_object_name, new_object_id)
            for (result_object_id, result_object_name), new_object_id in zip(result_objects, new_result_object_ids)
        ]

        await asyncio.gather(*futures)

        return new_result_object_ids

    async def __duplicate_object(
        self, owner: ClientID, object_id: ObjectID, object_name: bytes, new_object_id: ObjectID
    ):
        await self._connector_storage.duplicate_object_id(object_id, new_object_id)

        self._object_controller.on_add_object(
            owner, new_object_id, ObjectMetadata.ObjectContentType.object, object_name
        )

    async def __send_results(self, client_id: ClientID, results: List[TaskResult]):
        await asyncio.gather(*[self._binder.send(client_id, result) for result in results])

    async def __send_task_cancel_confirms(self, client_id: ClientID, task_cancel_confirms: List[TaskCancelConfirm]):
        await asyncio.gather(
            *[self._binder.send(client_id, task_cancel_confirm) for task_cancel_confirm in task_cancel_confirms]
        )
