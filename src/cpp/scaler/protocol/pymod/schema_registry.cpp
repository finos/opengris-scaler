#include "scaler/protocol/pymod/schema_registry.h"

#include <stdexcept>

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

// Pyodide's SIDE_MODULE relocator mis-resolves offsets within mergeable
// `.rodata.str1.1` sections, causing tail-merged short string literals to
// resolve to bytes inside longer literals at load time. Storing module names
// in dedicated `static const char[]` arrays forces the linker to allocate
// them in non-mergeable storage so the addresses remain valid.
namespace {
static const char kModCommon[]         = "common";
static const char kModStatus[]         = "status";
static const char kModMessage[]        = "message";
static const char kModObjectStorage[] = "object_storage";
}  // namespace

// Apply the same Pyodide SIDE_MODULE workaround documented above to *type
// name* literals: bare string literals like "ObjectRequestHeader" can be
// tail-merged into longer literals in `.rodata.str1.1` and resolve to wrong
// bytes after wasm relocation. Forcing each name into its own
// function-scoped `static const char[]` storage prevents the merge.
#define REG_STRUCT(MOD, T)                                       \
    do {                                                          \
        static const char kName_##T[] = #T;                       \
        registerCompiledSchema<scaler::protocol::T>(MOD, kName_##T); \
    } while (0)

bool SchemaRegistry::init()
{
    if (_initialized) {
        return true;
    }

    REG_STRUCT(kModCommon, TaskResultType);
    REG_STRUCT(kModCommon, TaskCancelConfirmType);
    REG_STRUCT(kModCommon, TaskTransition);
    REG_STRUCT(kModCommon, TaskState);
    REG_STRUCT(kModCommon, WorkerState);
    REG_STRUCT(kModCommon, TaskCapability);
    REG_STRUCT(kModCommon, ObjectMetadata);
    REG_STRUCT(kModCommon, ObjectStorageAddress);

    REG_STRUCT(kModStatus, Resource);
    REG_STRUCT(kModStatus, ObjectManagerStatus);
    REG_STRUCT(kModStatus, ClientManagerStatus);
    REG_STRUCT(kModStatus, TaskManagerStatus);
    REG_STRUCT(kModStatus, ProcessorStatus);
    REG_STRUCT(kModStatus, WorkerStatus);
    REG_STRUCT(kModStatus, WorkerManagerStatus);
    REG_STRUCT(kModStatus, ScalingManagerStatus);
    REG_STRUCT(kModStatus, BinderStatus);

    REG_STRUCT(kModMessage, Task);
    REG_STRUCT(kModMessage, TaskCancel);
    REG_STRUCT(kModMessage, TaskLog);
    REG_STRUCT(kModMessage, TaskResult);
    REG_STRUCT(kModMessage, TaskCancelConfirm);
    REG_STRUCT(kModMessage, GraphTask);
    REG_STRUCT(kModMessage, ClientHeartbeat);
    REG_STRUCT(kModMessage, ClientHeartbeatEcho);
    REG_STRUCT(kModMessage, WorkerHeartbeat);
    REG_STRUCT(kModMessage, WorkerHeartbeatEcho);
    REG_STRUCT(kModMessage, WorkerManagerHeartbeat);
    REG_STRUCT(kModMessage, WorkerManagerHeartbeatEcho);
    REG_STRUCT(kModMessage, WorkerManagerCommand);
    REG_STRUCT(kModMessage, ObjectInstruction);
    REG_STRUCT(kModMessage, DisconnectRequest);
    REG_STRUCT(kModMessage, DisconnectResponse);
    REG_STRUCT(kModMessage, ClientDisconnect);
    REG_STRUCT(kModMessage, ClientShutdownResponse);
    REG_STRUCT(kModMessage, StateClient);
    REG_STRUCT(kModMessage, StateObject);
    REG_STRUCT(kModMessage, StateBalanceAdvice);
    REG_STRUCT(kModMessage, StateScheduler);
    REG_STRUCT(kModMessage, StateWorker);
    REG_STRUCT(kModMessage, StateTask);
    REG_STRUCT(kModMessage, StateGraphTask);
    REG_STRUCT(kModMessage, ProcessorInitialized);
    REG_STRUCT(kModMessage, InformationRequest);
    REG_STRUCT(kModMessage, InformationResponse);
    REG_STRUCT(kModMessage, Message);

    REG_STRUCT(kModObjectStorage, ObjectRequestHeader);
    REG_STRUCT(kModObjectStorage, ObjectID);
    REG_STRUCT(kModObjectStorage, ObjectResponseHeader);

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
{
    return getSchemaById(schemaId).asStruct();
}

capnp::EnumSchema SchemaRegistry::getEnumById(uint64_t schemaId)
{
    return getSchemaById(schemaId).asEnum();
}

capnp::StructSchema SchemaRegistry::getStructByName(const std::string& typeName)
{
    auto type_id = _topLevelTypeIds.find(typeName);
    if (type_id != _topLevelTypeIds.end()) {
        return getStructById(type_id->second);
    }

    auto separator = typeName.rfind('.');
    if (separator == std::string::npos) {
        throw std::out_of_range("unknown Cap'n Proto struct type");
    }

    return getStructById(_topLevelTypeIds.at(typeName.substr(separator + 1)));
}

const std::vector<capnp::Schema>* SchemaRegistry::getModuleSchemas(const std::string& moduleName) const
{
    auto it = _moduleSchemas.find(moduleName);
    if (it == _moduleSchemas.end()) {
        return nullptr;
    }
    return &it->second;
}

}  // namespace scaler::protocol::pymod
