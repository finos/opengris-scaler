#include "scaler/protocol/pymod/schema_registry.h"

#include "protocol/common.capnp.h"
#include "protocol/message.capnp.h"
#include "protocol/object_storage.capnp.h"
#include "protocol/status.capnp.h"

namespace scaler::protocol::pymod {

template <typename T>
void SchemaRegistry::registerCompiledSchema(const char* moduleName, const char* typeName)
{
    _loader.loadCompiledTypeAndDependencies<T>();
    auto schema = capnp::Schema::from<T>();
    _moduleSchemas[moduleName].push_back(schema);
    _topLevelTypeIds.emplace(typeName, schema.getProto().getId());
}

bool SchemaRegistry::init()
{
    if (_initialized) {
        return true;
    }

    registerCompiledSchema<scaler::protocol::TaskResultType>("common", "TaskResultType");
    registerCompiledSchema<scaler::protocol::TaskCancelConfirmType>("common", "TaskCancelConfirmType");
    registerCompiledSchema<scaler::protocol::TaskTransition>("common", "TaskTransition");
    registerCompiledSchema<scaler::protocol::TaskState>("common", "TaskState");
    registerCompiledSchema<scaler::protocol::WorkerState>("common", "WorkerState");
    registerCompiledSchema<scaler::protocol::TaskCapability>("common", "TaskCapability");
    registerCompiledSchema<scaler::protocol::ObjectMetadata>("common", "ObjectMetadata");
    registerCompiledSchema<scaler::protocol::ObjectStorageAddress>("common", "ObjectStorageAddress");

    registerCompiledSchema<scaler::protocol::Resource>("status", "Resource");
    registerCompiledSchema<scaler::protocol::ObjectManagerStatus>("status", "ObjectManagerStatus");
    registerCompiledSchema<scaler::protocol::ClientManagerStatus>("status", "ClientManagerStatus");
    registerCompiledSchema<scaler::protocol::TaskManagerStatus>("status", "TaskManagerStatus");
    registerCompiledSchema<scaler::protocol::ProcessorStatus>("status", "ProcessorStatus");
    registerCompiledSchema<scaler::protocol::WorkerStatus>("status", "WorkerStatus");
    registerCompiledSchema<scaler::protocol::WorkerManagerStatus>("status", "WorkerManagerStatus");
    registerCompiledSchema<scaler::protocol::ScalingManagerStatus>("status", "ScalingManagerStatus");
    registerCompiledSchema<scaler::protocol::BinderStatus>("status", "BinderStatus");

    registerCompiledSchema<scaler::protocol::Task>("message", "Task");
    registerCompiledSchema<scaler::protocol::TaskCancel>("message", "TaskCancel");
    registerCompiledSchema<scaler::protocol::TaskLog>("message", "TaskLog");
    registerCompiledSchema<scaler::protocol::TaskResult>("message", "TaskResult");
    registerCompiledSchema<scaler::protocol::TaskCancelConfirm>("message", "TaskCancelConfirm");
    registerCompiledSchema<scaler::protocol::GraphTask>("message", "GraphTask");
    registerCompiledSchema<scaler::protocol::ClientHeartbeat>("message", "ClientHeartbeat");
    registerCompiledSchema<scaler::protocol::ClientHeartbeatEcho>("message", "ClientHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerHeartbeat>("message", "WorkerHeartbeat");
    registerCompiledSchema<scaler::protocol::WorkerHeartbeatEcho>("message", "WorkerHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerManagerHeartbeat>("message", "WorkerManagerHeartbeat");
    registerCompiledSchema<scaler::protocol::WorkerManagerHeartbeatEcho>("message", "WorkerManagerHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommandType>("message", "WorkerManagerCommandType");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommand>("message", "WorkerManagerCommand");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommandResponse>("message", "WorkerManagerCommandResponse");
    registerCompiledSchema<scaler::protocol::ObjectInstruction>("message", "ObjectInstruction");
    registerCompiledSchema<scaler::protocol::DisconnectRequest>("message", "DisconnectRequest");
    registerCompiledSchema<scaler::protocol::DisconnectResponse>("message", "DisconnectResponse");
    registerCompiledSchema<scaler::protocol::ClientDisconnect>("message", "ClientDisconnect");
    registerCompiledSchema<scaler::protocol::ClientShutdownResponse>("message", "ClientShutdownResponse");
    registerCompiledSchema<scaler::protocol::StateClient>("message", "StateClient");
    registerCompiledSchema<scaler::protocol::StateObject>("message", "StateObject");
    registerCompiledSchema<scaler::protocol::StateBalanceAdvice>("message", "StateBalanceAdvice");
    registerCompiledSchema<scaler::protocol::StateScheduler>("message", "StateScheduler");
    registerCompiledSchema<scaler::protocol::StateWorker>("message", "StateWorker");
    registerCompiledSchema<scaler::protocol::StateTask>("message", "StateTask");
    registerCompiledSchema<scaler::protocol::StateGraphTask>("message", "StateGraphTask");
    registerCompiledSchema<scaler::protocol::ProcessorInitialized>("message", "ProcessorInitialized");
    registerCompiledSchema<scaler::protocol::InformationRequest>("message", "InformationRequest");
    registerCompiledSchema<scaler::protocol::InformationResponse>("message", "InformationResponse");
    registerCompiledSchema<scaler::protocol::Message>("message", "Message");

    registerCompiledSchema<scaler::protocol::ObjectRequestHeader>("object_storage", "ObjectRequestHeader");
    registerCompiledSchema<scaler::protocol::ObjectID>("object_storage", "ObjectID");
    registerCompiledSchema<scaler::protocol::ObjectResponseHeader>("object_storage", "ObjectResponseHeader");

    for (const auto& schema: _loader.getAllLoaded()) {
        _schemasById.emplace(schema.getProto().getId(), schema);
    }

    _initialized = true;
    return true;
}

capnp::Schema SchemaRegistry::getSchemaById(uint64_t schemaId)
{
    init();
    return _schemasById.at(schemaId);
}

capnp::StructSchema SchemaRegistry::getStructById(uint64_t schemaId)
{ return getSchemaById(schemaId).asStruct(); }

capnp::EnumSchema SchemaRegistry::getEnumById(uint64_t schemaId)
{ return getSchemaById(schemaId).asEnum(); }

capnp::StructSchema SchemaRegistry::getStructByName(const std::string& typeName)
{ return getStructById(_topLevelTypeIds.at(typeName)); }

const std::vector<capnp::Schema>* SchemaRegistry::getModuleSchemas(const std::string& moduleName) const
{
    auto it = _moduleSchemas.find(moduleName);
    if (it == _moduleSchemas.end()) {
        return nullptr;
    }
    return &it->second;
}

}  // namespace scaler::protocol::pymod
