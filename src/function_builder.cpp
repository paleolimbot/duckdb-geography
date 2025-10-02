#include "function_builder.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"

namespace duckdb {

void FunctionBuilder::Register(ExtensionLoader& loader, const char* name,
                               ScalarFunctionBuilder& builder) {
  // Register the function
  loader.RegisterFunction(std::move(builder.set));

  auto& db = loader.GetDatabaseInstance();

  // Also add the parameter names. We need to access the catalog entry for this.
  auto& catalog = Catalog::GetSystemCatalog(db);
  auto transaction = CatalogTransaction::GetSystemTransaction(db);
  auto& schema = catalog.GetSchema(transaction, DEFAULT_SCHEMA);
  auto catalog_entry =
      schema.GetEntry(transaction, CatalogType::SCALAR_FUNCTION_ENTRY, name);
  if (!catalog_entry) {
    // This should not happen, we just registered the function
    throw InternalException(
        "Function with name \"%s\" not found in FunctionBuilder::AddScalar", name);
  }

  auto& func_entry = catalog_entry->Cast<FunctionEntry>();

#if defined(DUCKDB_FUNC_ENTRY_HAS_METADATA)
  if (!builder.parameter_names.empty()) {
    func_entry.parameter_names = std::move(builder.parameter_names);
  }

  if (!builder.description.empty()) {
    func_entry.description = std::move(builder.description);
  }

  if (!builder.example.empty()) {
    func_entry.example = std::move(builder.example);
  }
#endif
  if (!builder.tags.empty()) {
    func_entry.tags = std::move(builder.tags);
  }
}

}  // namespace duckdb
