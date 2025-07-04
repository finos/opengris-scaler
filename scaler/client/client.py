import dataclasses
import functools
import logging
import threading
import uuid
from collections import Counter
from inspect import signature
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import zmq

from scaler.client.agent.client_agent import ClientAgent
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.future import ScalerFuture
from scaler.client.object_buffer import ObjectBuffer
from scaler.client.object_reference import ObjectReference
from scaler.client.serializer.default import DefaultSerializer
from scaler.client.serializer.mixins import Serializer
from scaler.io.config import DEFAULT_CLIENT_TIMEOUT_SECONDS, DEFAULT_HEARTBEAT_INTERVAL_SECONDS
from scaler.io.sync_connector import SyncConnector
from scaler.io.sync_object_storage_connector import SyncObjectStorageConnector
from scaler.protocol.python.message import ClientDisconnect, ClientShutdownResponse, GraphTask, Task
from scaler.utility.exceptions import ClientQuitException, MissingObjects
from scaler.utility.graph.optimization import cull_graph
from scaler.utility.graph.topological_sorter import TopologicalSorter
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.metadata.task_flags import TaskFlags, retrieve_task_flags_from_task
from scaler.utility.zmq_config import ZMQConfig, ZMQType
from scaler.worker.agent.processor.processor import Processor


@dataclasses.dataclass
class _CallNode:
    func: Callable
    args: Tuple[str, ...]

    def __post_init__(self):
        if not callable(self.func):
            raise TypeError(f"the first item of the tuple must be function, get {self.func}")

        if not isinstance(self.args, tuple):
            raise TypeError(f"arguments must be tuple, get {self.args}")

        for arg in self.args:
            if not isinstance(arg, str):
                raise TypeError(f"argument `{arg}` must be a string and the string has to be in the graph")


class Client:
    def __init__(
        self,
        address: str,
        profiling: bool = False,
        timeout_seconds: int = DEFAULT_CLIENT_TIMEOUT_SECONDS,
        heartbeat_interval_seconds: int = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        serializer: Serializer = DefaultSerializer(),
    ):
        """
        The Scaler Client used to send tasks to a scheduler.

        :param address: Address of Scheduler to submit work to
        :type address: str
        :param profiling: If True, the returned futures will have the `task_duration()` property enabled.
        :type profiling: bool
        :param timeout_seconds: Seconds until heartbeat times out
        :type timeout_seconds: int
        :param heartbeat_interval_seconds: Frequency of heartbeat to scheduler in seconds
        :type heartbeat_interval_seconds: int
        """
        self.__initialize__(address, profiling, timeout_seconds, heartbeat_interval_seconds, serializer)

    def __initialize__(
        self,
        address: str,
        profiling: bool,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer = DefaultSerializer(),
    ):
        self._serializer = serializer

        self._profiling = profiling
        self._identity = ClientID.generate_client_id()

        self._client_agent_address = ZMQConfig(ZMQType.inproc, host=f"scaler_client_{uuid.uuid4().hex}")
        self._scheduler_address = ZMQConfig.from_string(address)
        self._timeout_seconds = timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds

        self._stop_event = threading.Event()
        self._context = zmq.Context()
        self._connector_agent = SyncConnector(
            context=self._context, socket_type=zmq.PAIR, address=self._client_agent_address, identity=self._identity
        )

        self._future_manager = ClientFutureManager(self._serializer)
        self._agent = ClientAgent(
            identity=self._identity,
            client_agent_address=self._client_agent_address,
            scheduler_address=ZMQConfig.from_string(address),
            context=self._context,
            future_manager=self._future_manager,
            stop_event=self._stop_event,
            timeout_seconds=self._timeout_seconds,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            serializer=self._serializer,
        )
        self._agent.start()

        logging.info(f"ScalerClient: connect to scheduler at {self._scheduler_address}")

        # Blocks until the agent receives the object storage address
        self._storage_address = self._agent.get_storage_address()

        logging.info(f"ScalerClient: connect to object storage at {self._storage_address}")
        self._connector_storage = SyncObjectStorageConnector(self._storage_address.host, self._storage_address.port)

        self._object_buffer = ObjectBuffer(
            self._identity,
            self._serializer,
            self._connector_agent,
            self._connector_storage,
        )
        self._future_factory = functools.partial(
            ScalerFuture,
            serializer=self._serializer,
            connector_agent=self._connector_agent,
            connector_storage=self._connector_storage
        )

    @property
    def identity(self) -> ClientID:
        return self._identity

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __getstate__(self) -> dict:
        """
        Serializes the client object's state.

        Client serialization is useful when a client reference is used within a remote task:


        .. code:: python

            client = Client(...)

            def fibonacci(client: Client, n: int):
                if n == 0:
                    return 0
                elif n == 1:
                    return 1
                else:
                    a = client.submit(fibonacci, n - 1)
                    b = client.submit(fibonacci, n - 2)
                    return a.result() + b.result()

            print(client.submit(fibonacci, client, 7).result())


        When serializing the client, only saves the address parameters. When deserialized, a new client object
        connecting to the same scheduler and remote logger will be instantiated.
        """

        return {
            "address": self._scheduler_address.to_address(),
            "profiling": self._profiling,
            "timeout_seconds": self._timeout_seconds,
            "heartbeat_interval_seconds": self._heartbeat_interval_seconds,
        }

    def __setstate__(self, state: dict) -> None:
        # TODO: fix copy the serializer
        self.__initialize__(
            address=state["address"],
            profiling=state["profiling"],
            timeout_seconds=state["timeout_seconds"],
            heartbeat_interval_seconds=state["heartbeat_interval_seconds"],
        )

    def submit(self, fn: Callable, *args, **kwargs) -> ScalerFuture:
        """
        Submit a single task (function with arguments) to the scheduler, and return a future

        :param fn: function to be executed remotely
        :type fn: Callable
        :param args: positional arguments will be passed to function
        :return: future of the submitted task
        :rtype: ScalerFuture
        """

        self.__assert_client_not_stopped()

        function_object_id = self._object_buffer.buffer_send_function(fn).object_id
        all_args = Client.__convert_kwargs_to_args(fn, args, kwargs)

        task, future = self.__submit(function_object_id, all_args, delayed=True)

        self._object_buffer.commit_send_objects()
        self._connector_agent.send(task)
        return future

    def map(self, fn: Callable, iterable: Iterable[Tuple[Any, ...]]) -> List[Any]:
        if not all(isinstance(args, (tuple, list)) for args in iterable):
            raise TypeError("iterable should be list of arguments(list or tuple-like) of function")

        self.__assert_client_not_stopped()

        function_object_id = self._object_buffer.buffer_send_function(fn).object_id
        tasks, futures = zip(*[self.__submit(function_object_id, args, delayed=False) for args in iterable])

        self._object_buffer.commit_send_objects()
        for task in tasks:
            self._connector_agent.send(task)

        try:
            results = [fut.result() for fut in futures]
        except Exception as e:
            logging.exception(f"error happened when do scaler client.map:\n{e}")
            self.disconnect()
            raise e

        return results

    def get(
        self, graph: Dict[str, Union[Any, Tuple[Union[Callable, str], ...]]], keys: List[str], block: bool = True
    ) -> Dict[str, Union[Any, ScalerFuture]]:
        """
        .. code-block:: python
           :linenos:
            graph = {
                "a": 1,
                "b": 2,
                "c": (inc, "a"),
                "d": (inc, "b"),
                "e": (add, "c", "d")
            }

        :param graph: dictionary presentation of task graphs
        :type graph: Dict[str, Union[Any, Tuple[Union[Callable, Any]]
        :param keys: list of keys want to get results from computed graph
        :type keys: List[str]
        :param block: if True, it will directly return a dictionary that maps from keys to results
        :return: dictionary of mapping keys to futures, or map to results if block=True is specified
        :rtype: Dict[ScalerFuture]
        """

        self.__assert_client_not_stopped()

        graph = cull_graph(graph, keys)

        node_name_to_argument, call_graph = self.__split_data_and_graph(graph)
        self.__check_graph(node_name_to_argument, call_graph, keys)

        graph_task, compute_futures, finished_futures = self.__construct_graph(
            node_name_to_argument, call_graph, keys, block
        )
        self._object_buffer.commit_send_objects()
        self._connector_agent.send(graph_task)

        self._future_manager.add_future(
            self._future_factory(
                task=Task.new_msg(
                    task_id=graph_task.task_id,
                    source=self._identity,
                    metadata=b"",
                    func_object_id=None,
                    function_args=[],
                ),
                is_delayed=not block,
                group_task_id=None,
            )
        )
        for future in compute_futures.values():
            self._future_manager.add_future(future)

        # preserve the future insertion order based on inputted keys
        futures = {}
        for key in keys:
            if key in compute_futures:
                futures[key] = compute_futures[key]
            else:
                futures[key] = finished_futures[key]

        if not block:
            # just return futures
            return futures

        try:
            results = {k: v.result() for k, v in futures.items()}
        except Exception as e:
            logging.exception(f"error happened when do scaler client.get:\n{e}")
            self.disconnect()
            raise e

        return results

    def send_object(self, obj: Any, name: Optional[str] = None) -> ObjectReference:
        """
        send object to scheduler, this can be used to cache very large data to scheduler, and reuse it in multiple
        tasks

        :param obj: object to send, it will be serialized and send to scheduler
        :type obj: Any
        :param name: give a name to the cached argument
        :type name: Optional[str]
        :return: object reference
        :rtype ObjectReference
        """

        self.__assert_client_not_stopped()

        cache = self._object_buffer.buffer_send_object(obj, name)
        return ObjectReference(cache.object_name, len(cache.object_payload), cache.object_id)

    def clear(self):
        """
        clear all resources used by the client, this will cancel all running futures and invalidate all existing object
        references
        """

        # It's important to be ensure that all running futures are cancelled/finished before clearing object, or else we
        # might end up with tasks indefinitely waiting on no longer existing objects.
        self._future_manager.cancel_all_futures()

        self._object_buffer.clear()

    def disconnect(self):
        """
        disconnect from connected scheduler, this will not shut down the scheduler
        """

        if self._stop_event.is_set():
            self.__destroy()
            return

        logging.info(f"ScalerClient: disconnect from {self._scheduler_address.to_address()}")

        self._future_manager.cancel_all_futures()

        self._connector_agent.send(ClientDisconnect.new_msg(ClientDisconnect.DisconnectType.Disconnect))

        self.__destroy()

    def __receive_shutdown_response(self):
        message: Optional[ClientShutdownResponse] = None
        while not isinstance(message, ClientShutdownResponse):
            message = self._connector_agent.receive()

        if not message.accepted:
            raise ValueError("Scheduler is in protected mode. Can't shutdown")

    def shutdown(self):
        """
        shutdown all workers that connected to the scheduler this client connects to, it will cancel all other
        clients' ongoing tasks, please be aware shutdown might not success if scheduler is configured as protected mode,
        then it cannot shut down scheduler and the workers
        """

        if not self._agent.is_alive():
            self.__destroy()
            return

        logging.info(f"ScalerClient: request shutdown for {self._scheduler_address.to_address()}")

        self._future_manager.cancel_all_futures()

        self._connector_agent.send(ClientDisconnect.new_msg(ClientDisconnect.DisconnectType.Shutdown))
        try:
            self.__receive_shutdown_response()
        finally:
            self.__destroy()

    def __submit(self, function_object_id: ObjectID, args: Tuple[Any, ...], delayed: bool) -> Tuple[Task, ScalerFuture]:
        task_id = TaskID.generate_task_id()

        function_args: List[Union[ObjectID, TaskID]] = []
        for arg in args:
            if isinstance(arg, ObjectReference):
                if not self._object_buffer.is_valid_object_id(arg.object_id):
                    raise MissingObjects(f"unknown object: {arg.object_id!r}.")

                function_args.append(arg.object_id)
            else:
                function_args.append(self._object_buffer.buffer_send_object(arg).object_id)

        task_flags_bytes = self.__get_task_flags().serialize()

        task = Task.new_msg(
            task_id=task_id,
            source=self._identity,
            metadata=task_flags_bytes,
            func_object_id=function_object_id,
            function_args=function_args,
        )

        future = self._future_factory(task=task, is_delayed=delayed, group_task_id=None)
        self._future_manager.add_future(future)
        return task, future

    @staticmethod
    def __convert_kwargs_to_args(fn: Callable, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Any, ...]:
        all_params = [p for p in signature(fn).parameters.values()]

        params = [p for p in all_params if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}]

        if len(args) >= len(params):
            return args

        number_of_required = len([p for p in params if p.default is p.empty])

        args_list = list(args)
        kwargs = kwargs.copy()
        kwargs.update({p.name: p.default for p in all_params if p.kind == p.KEYWORD_ONLY if p.default != p.empty})

        for p in params[len(args_list) : number_of_required]:
            try:
                args_list.append(kwargs.pop(p.name))
            except KeyError:
                missing = tuple(p.name for p in params[len(args_list) : number_of_required])
                raise TypeError(f"{fn} missing {len(missing)} arguments: {missing}")

        for p in params[len(args_list) :]:
            args_list.append(kwargs.pop(p.name, p.default))

        return tuple(args_list)

    def __split_data_and_graph(
        self, graph: Dict[str, Union[Any, Tuple[Union[Callable, str], ...]]]
    ) -> Tuple[Dict[str, Tuple[ObjectID, Any]], Dict[str, _CallNode]]:
        call_graph = {}
        node_name_to_argument: Dict[str, Tuple[ObjectID, Union[Any, Tuple[Union[Callable, Any], ...]]]] = dict()

        for node_name, node in graph.items():
            if isinstance(node, tuple) and len(node) > 0 and callable(node[0]):
                call_graph[node_name] = _CallNode(func=node[0], args=node[1:])  # type: ignore[arg-type]
                continue

            if isinstance(node, ObjectReference):
                object_id = node.object_id
            else:
                object_id = self._object_buffer.buffer_send_object(node, name=node_name).object_id

            node_name_to_argument[node_name] = (object_id, node)

        return node_name_to_argument, call_graph

    @staticmethod
    def __check_graph(
        node_to_argument: Dict[str, Tuple[ObjectID, Any]], call_graph: Dict[str, _CallNode], keys: List[str]
    ):
        duplicate_keys = [key for key, count in dict(Counter(keys)).items() if count > 1]
        if duplicate_keys:
            raise KeyError(f"duplicate key detected in argument keys: {duplicate_keys}")

        # sanity check graph
        for key in keys:
            if key not in call_graph and key not in node_to_argument:
                raise KeyError(f"key {key} has to be in graph")

        sorter: TopologicalSorter[str] = TopologicalSorter()
        for node_name, node in call_graph.items():
            for arg in node.args:
                if arg not in node_to_argument and arg not in call_graph:
                    raise KeyError(f"argument {arg} in node '{node_name}': {node} is not defined in graph")

            sorter.add(node_name, *node.args)

        # check cyclic dependencies
        sorter.prepare()

    def __construct_graph(
        self,
        node_name_to_arguments: Dict[str, Tuple[ObjectID, Any]],
        call_graph: Dict[str, _CallNode],
        keys: List[str],
        block: bool,
    ) -> Tuple[GraphTask, Dict[str, ScalerFuture], Dict[str, ScalerFuture]]:
        graph_task_id = TaskID.generate_task_id()

        node_name_to_task_id = {node_name: TaskID.generate_task_id() for node_name in call_graph.keys()}

        task_flags_bytes = self.__get_task_flags().serialize()

        task_id_to_tasks = dict()

        for node_name, node in call_graph.items():
            task_id = node_name_to_task_id[node_name]
            function_cache = self._object_buffer.buffer_send_function(node.func)

            arguments: List[Union[TaskID, ObjectID]] = []
            for arg in node.args:
                assert arg in call_graph or arg in node_name_to_arguments

                if arg in call_graph:
                    arguments.append(TaskID(node_name_to_task_id[arg]))
                elif arg in node_name_to_arguments:
                    argument, _ = node_name_to_arguments[arg]
                    arguments.append(argument)
                else:
                    raise ValueError("Not possible")

            task_id_to_tasks[task_id] = Task.new_msg(
                task_id=task_id,
                source=self._identity,
                metadata=task_flags_bytes,
                func_object_id=function_cache.object_id,
                function_args=arguments,
            )

        result_task_ids = [node_name_to_task_id[key] for key in keys if key in call_graph]
        graph_task = GraphTask.new_msg(graph_task_id, self._identity, result_task_ids, list(task_id_to_tasks.values()))

        compute_futures = {}
        ready_futures = {}
        for key in keys:
            if key in call_graph:
                compute_futures[key] = self._future_factory(
                    task=task_id_to_tasks[node_name_to_task_id[key]], is_delayed=not block, group_task_id=graph_task_id
                )

            elif key in node_name_to_arguments:
                argument, data = node_name_to_arguments[key]
                future: ScalerFuture = self._future_factory(
                    task=Task.new_msg(
                        task_id=TaskID.generate_task_id(),
                        source=self._identity,
                        metadata=b"",
                        func_object_id=None,
                        function_args=[],
                    ),
                    is_delayed=False,
                    group_task_id=graph_task_id,
                )
                future.set_result(data, ProfileResult())
                ready_futures[key] = future

            else:
                raise ValueError(f"cannot find {key=} in graph")

        return graph_task, compute_futures, ready_futures

    def __get_task_flags(self) -> TaskFlags:
        parent_task_priority = self.__get_parent_task_priority()

        if parent_task_priority is not None:
            task_priority = parent_task_priority + 1
        else:
            task_priority = 0

        return TaskFlags(profiling=self._profiling, priority=task_priority)

    def __assert_client_not_stopped(self):
        if self._stop_event.is_set():
            raise ClientQuitException("client is already stopped.")

    def __destroy(self):
        self._agent.join()
        self._context.destroy(linger=1)

    @staticmethod
    def __get_parent_task_priority() -> Optional[int]:
        """If the client is running inside a Scaler processor, returns the priority of the associated task."""

        current_processor = Processor.get_current_processor()

        if current_processor is None:
            return None

        current_task = current_processor.current_task()
        assert current_task is not None

        return retrieve_task_flags_from_task(current_task).priority
