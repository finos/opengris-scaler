#pragma once

#include <capnp/schema-loader.h>
#include <capnp/schema.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace scaler::protocol::pymod {

class SchemaRegistry {
public:
    bool init();

    capnp::Schema getSchemaById(uint64_t schemaId);
    capnp::StructSchema getStructById(uint64_t schemaId);
    capnp::EnumSchema getEnumById(uint64_t schemaId);
    capnp::StructSchema getStructByName(const std::string& typeName);
    const std::vector<capnp::Schema>* getModuleSchemas(const std::string& moduleName) const;

private:
    template <typename T>
    void registerCompiledSchema(const char* moduleName, const char* typeName);

    bool _initialized = false;
    capnp::SchemaLoader _loader;
    std::unordered_map<uint64_t, capnp::Schema> _schemasById;
    std::unordered_map<std::string, std::vector<capnp::Schema>> _moduleSchemas;
    std::unordered_map<std::string, uint64_t> _topLevelTypeIds;
};

}  // namespace scaler::protocol::pymod
