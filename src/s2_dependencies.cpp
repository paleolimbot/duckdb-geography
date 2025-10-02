#include "duckdb/function/table_function.hpp"

#include <absl/base/config.h>
#include <openssl/opensslv.h>
#include <s2geography.h>

#include "duckdb/main/extension/extension_loader.hpp"
#include "geoarrow/geoarrow.h"
#include "nanoarrow/nanoarrow.h"

namespace duckdb {

namespace duckdb_s2 {

namespace {
class S2DependenciesFunctionData : public TableFunctionData {
 public:
  S2DependenciesFunctionData() : finished(false) {}
  bool finished{false};
};

static inline duckdb::unique_ptr<FunctionData> S2DependenciesBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("dependency");
  names.push_back("version");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::VARCHAR);
  return make_uniq<S2DependenciesFunctionData>();
}

void S2DependenciesScan(ClientContext& context, TableFunctionInput& data_p,
                        DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DependenciesFunctionData>();
  if (data.finished) {
    return;
  }

  output.SetValue(0, 0, "openssl");
  output.SetValue(1, 0,
                  std::string() + std::to_string(OPENSSL_VERSION_MAJOR) + "." +
                      std::to_string(OPENSSL_VERSION_MINOR) + "." +
                      std::to_string(OPENSSL_VERSION_PATCH));
  output.SetValue(0, 1, "abseil-cpp");
  output.SetValue(1, 1,
                  std::string() + std::to_string(ABSL_LTS_RELEASE_VERSION) + "." +
                      std::to_string(ABSL_LTS_RELEASE_PATCH_LEVEL));
  output.SetValue(0, 2, "s2geometry");
  output.SetValue(1, 2,
                  std::string() + std::to_string(S2_VERSION_MAJOR) + "." +
                      std::to_string(S2_VERSION_MINOR) + "." +
                      std::to_string(S2_VERSION_PATCH));

  output.SetValue(0, 3, "nanoarrow");
  output.SetValue(1, 3, std::string() + ArrowNanoarrowVersion());

  output.SetValue(0, 4, "geoarrow");
  output.SetValue(1, 4, std::string() + GeoArrowVersion());

  output.SetCardinality(5);
  data.finished = true;
}

}  // namespace

void RegisterS2Dependencies(ExtensionLoader& loader) {
  TableFunction versions_func("s2_dependencies", {}, S2DependenciesScan,
                              S2DependenciesBind);
  loader.RegisterFunction(versions_func);
}

}  // namespace duckdb_s2
}  // namespace duckdb
