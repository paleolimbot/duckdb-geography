#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"

#include <absl/base/config.h>
#include <s2geography.h>

#include "s2_data_static.hpp"
#include "s2_geography_serde.hpp"
#include "s2_types.hpp"

#include "function_builder.hpp"

namespace duckdb {

namespace duckdb_s2 {

namespace {

class S2DataFunctionData : public TableFunctionData {
 public:
  S2DataFunctionData() {}
  idx_t offset{0};
};

static inline duckdb::unique_ptr<FunctionData> S2DataCitiesBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("name");
  names.push_back("population");
  names.push_back("geog");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::INTEGER);
  return_types.push_back(Types::GEOGRAPHY());
  return make_uniq<S2DataFunctionData>();
}

void S2DataCitiesScan(ClientContext& context, TableFunctionInput& data_p,
                      DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DataFunctionData>();
  idx_t n_cities = static_cast<idx_t>(kCities.size());

  if (data.offset >= n_cities) {
    return;
  }

  idx_t start = data.offset;
  idx_t end = start + STANDARD_VECTOR_SIZE;
  if (end > n_cities) {
    end = n_cities;
  }

  s2geography::WKTReader reader;
  GeographyEncoder encoder;
  Vector& names = output.data[0];
  Vector& populations = output.data[1];
  Vector& geogs = output.data[2];

  // There seems to be some issue with constructing a Value from
  // invalid unicode (i.e., a blob), and it's unclear if SetValue()
  // will automatically call AddString(). So, we do this manually.
  auto geogs_data = reinterpret_cast<string_t*>(geogs.GetData());

  for (idx_t i = start; i < end; i++) {
    const City& city = kCities[i];
    names.SetValue(i - start, StringVector::AddString(names, city.name));
    populations.SetValue(i - start, city.population);

    auto geog = reader.read_feature(city.geog_wkt);
    string_t encoded = StringVector::AddStringOrBlob(geogs, encoder.Encode(*geog));
    geogs_data[i] = encoded;
  }

  data.offset = end;
  output.SetCardinality(end - start);
}

static inline duckdb::unique_ptr<FunctionData> S2DataCountriesBind(
    ClientContext& context, TableFunctionBindInput& input,
    vector<LogicalType>& return_types, vector<string>& names) {
  names.push_back("name");
  names.push_back("continent");
  names.push_back("geog");
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(LogicalType::VARCHAR);
  return_types.push_back(Types::GEOGRAPHY());
  return make_uniq<S2DataFunctionData>();
}

void S2DataCountriesScan(ClientContext& context, TableFunctionInput& data_p,
                         DataChunk& output) {
  auto& data = data_p.bind_data->CastNoConst<S2DataFunctionData>();
  idx_t n_cities = static_cast<idx_t>(kCountries.size());

  if (data.offset >= n_cities) {
    return;
  }

  idx_t start = data.offset;
  idx_t end = start + STANDARD_VECTOR_SIZE;
  if (end > n_cities) {
    end = n_cities;
  }

  s2geography::WKTReader reader;
  GeographyEncoder encoder;
  Vector& names = output.data[0];
  Vector& continents = output.data[1];
  Vector& geogs = output.data[2];

  // There seems to be some issue with constructing a Value from
  // invalid unicode (i.e., a blob), and it's unclear if SetValue()
  // will automatically call AddString(). So, we do this manually.
  auto geogs_data = reinterpret_cast<string_t*>(geogs.GetData());

  for (idx_t i = start; i < end; i++) {
    const Country& country = kCountries[i];
    names.SetValue(i - start, StringVector::AddString(names, country.name));
    continents.SetValue(i - start, StringVector::AddString(names, country.continent));

    auto geog = reader.read_feature(country.geog_wkt);
    string_t encoded = StringVector::AddStringOrBlob(geogs, encoder.Encode(*geog));
    geogs_data[i] = encoded;
  }

  data.offset = end;
  output.SetCardinality(end - start);
}

template <typename T>
const std::vector<T>& ItemList();

template <>
const std::vector<Country>& ItemList() {
  return kCountries;
}

template <>
const std::vector<City>& ItemList() {
  return kCities;
}

template <typename T>
struct S2DataScalar {
  static void Register(ExtensionLoader& loader, const char* fn_name) {
    FunctionBuilder::RegisterScalar(loader, fn_name, [](ScalarFunctionBuilder& func) {
      func.AddVariant([](ScalarFunctionVariantBuilder& variant) {
        variant.AddParameter("name", LogicalType::VARCHAR);
        variant.SetReturnType(Types::GEOGRAPHY());
        variant.SetFunction(ExecuteFn);
      });

      func.SetDescription(R"(
Get an example city or country from [`s2_data_cities()`](#s2_data_cities)
or [`s2_data_countries()`](#s2_data_countries) by name.
)");
      func.SetExample(R"(
SELECT s2_data_city('Toronto') as city;
----
SELECT s2_data_country('Fiji') as country;
)");

      func.SetTag("ext", "geography");
      func.SetTag("category", "data");
    });
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    s2geography::WKTReader reader;
    GeographyEncoder encoder;

    std::unordered_map<std::string, const char*> cache;
    for (const T& item : ItemList<T>()) {
      cache.insert({item.name, item.geog_wkt});
    }

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(), [&](string_t name) {
          std::string name_str(name.GetData(), name.GetSize());
          auto item = cache.find(name_str);
          if (item == cache.end()) {
            throw InvalidInputException(std::string("No entry for item '") + name_str +
                                        "'");
          }

          auto geog = reader.read_feature(item->second);
          return StringVector::AddStringOrBlob(result, encoder.Encode(*geog));
        });
  }
};

}  // namespace

void RegisterS2Data(ExtensionLoader& loader) {
  TableFunction cities_func("s2_data_cities", {}, S2DataCitiesScan, S2DataCitiesBind);
  loader.RegisterFunction(cities_func);

  TableFunction countries_func("s2_data_countries", {}, S2DataCountriesScan,
                               S2DataCountriesBind);
  loader.RegisterFunction(countries_func);

  S2DataScalar<City>::Register(loader, "s2_data_city");
  S2DataScalar<Country>::Register(loader, "s2_data_country");
}

}  // namespace duckdb_s2
}  // namespace duckdb
