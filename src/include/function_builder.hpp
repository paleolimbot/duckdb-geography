#pragma once

#include "duckdb.hpp"

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Scalar Function Variant Builder
//------------------------------------------------------------------------------

class ScalarFunctionVariantBuilder {
  friend class ScalarFunctionBuilder;

 public:
  void AddParameter(const char* name, LogicalType type);
  void SetReturnType(LogicalType type);
  void SetFunction(scalar_function_t fn);

 private:
  explicit ScalarFunctionVariantBuilder()
      : function({}, LogicalTypeId::INVALID, nullptr) {}

  ScalarFunction function;

  vector<string> parameter_names = {};
};

inline void ScalarFunctionVariantBuilder::AddParameter(const char* name,
                                                       LogicalType type) {
  function.arguments.emplace_back(std::move(type));
  parameter_names.emplace_back(name);
}

inline void ScalarFunctionVariantBuilder::SetReturnType(LogicalType type) {
  function.return_type = std::move(type);
}

inline void ScalarFunctionVariantBuilder::SetFunction(scalar_function_t fn) {
  function.function = fn;
}

//------------------------------------------------------------------------------
// Scalar Function Builder
//------------------------------------------------------------------------------

class ScalarFunctionBuilder {
  friend class FunctionBuilder;

 public:
  template <class CALLBACK>
  void AddVariant(CALLBACK&& callback);
  void SetDescription(const string& desc);
  void SetExample(const string& ex);
  void SetTag(const string& key, const string& value);

 private:
  explicit ScalarFunctionBuilder(const char* name) : set(name) {}

  ScalarFunctionSet set;

  vector<string> parameter_names;
  string description;
  string example;
  InsertionOrderPreservingMap<string> tags = {};
};

inline void ScalarFunctionBuilder::SetDescription(const string& desc) {
  description = desc;
}

inline void ScalarFunctionBuilder::SetExample(const string& ex) { example = ex; }

inline void ScalarFunctionBuilder::SetTag(const string& key, const string& value) {
  tags[key] = value;
}

template <class CALLBACK>
void ScalarFunctionBuilder::AddVariant(CALLBACK&& callback) {
  ScalarFunctionVariantBuilder builder;

  callback(builder);

  // A return type is required
  if (builder.function.return_type.id() == LogicalTypeId::INVALID) {
    throw InternalException("Return type not set in ScalarFunctionBuilder::AddVariant");
  }

  // Add the new variant to the set
  set.AddFunction(std::move(builder.function));

  // DuckDB does not support naming individual parameters differently between overloads,
  // there is only a single list of parameter names for the entire function.
  // Therefore, our only option right now is to append the new parameter names to the
  // list. This is going to change in DuckDB 1.2 where overloads will be able to have
  // different parameter names.

  // Add any new parameter names to the list
  const auto& old_params = parameter_names;
  const auto& new_params = builder.parameter_names;

  for (idx_t offset = old_params.size(); offset < new_params.size(); offset++) {
    parameter_names.emplace_back(builder.parameter_names[offset]);
  }
}

//------------------------------------------------------------------------------
// Function Builder
//------------------------------------------------------------------------------

class FunctionBuilder {
 public:
  template <class CALLBACK>
  static void RegisterScalar(ExtensionLoader& loader, const char* name,
                             CALLBACK&& callback);

 private:
  static void Register(ExtensionLoader& loader, const char* name,
                       ScalarFunctionBuilder& builder);
};

template <class CALLBACK>
void FunctionBuilder::RegisterScalar(ExtensionLoader& loader, const char* name,
                                     CALLBACK&& callback) {
  ScalarFunctionBuilder builder(name);
  callback(builder);

  Register(loader, name, builder);
}

}  // namespace duckdb
