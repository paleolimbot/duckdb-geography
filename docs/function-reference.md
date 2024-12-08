
# Function Reference

| Function | Summary |
| --- | --- |
| [`s2_area`](#s2_area) | Calculate the area of the geography in square meters.|
| [`s2_isempty`](#s2_isempty) | Returns true if the geography is empty.|
| [`s2_length`](#s2_length) | Calculate the length of the geography in meters.|
| [`s2_perimeter`](#s2_perimeter) | Calculate the perimeter of the geography in meters.|
| [`s2_x`](#s2_x) | Extract the longitude of a point geography.|
| [`s2_y`](#s2_y) | Extract the latitude of a point geography.|
| [`s2_bounds_rect`](#s2_bounds_rect) | Returns the bounds of the input geography as a box with Cartesian edges.|
| [`s2_box_struct`](#s2_box_struct) | Return a BOX_LNGLAT storage as a struct(xmin, ymin, xmax, ymax).|
| [`s2_box_wkb`](#s2_box_wkb) | Serialize a BOX_LNGLAT as WKB for export.|
| [`s2_covering`](#s2_covering) | Returns the S2 cell covering of the geography.|
| [`s2_covering_fixed_level`](#s2_covering_fixed_level) | Returns the S2 cell covering of the geography with a fixed level.|
| [`s2_arbitrarycellfromwkb`](#s2_arbitrarycellfromwkb) | Get an arbitrary S2_CELL_CENTER on or near the input.|
| [`s2_cell_child`](#s2_cell_child) | Compute a child S2_CELL.|
| [`s2_cell_contains`](#s2_cell_contains) | Return true if `cell1` contains `cell2`.|
| [`s2_cell_edge_neighbor`](#s2_cell_edge_neighbor) | Compute a neighbor S2_CELL.|
| [`s2_cell_from_token`](#s2_cell_from_token) | Parse a hexadecimal token as an S2_CELL.|
| [`s2_cell_intersects`](#s2_cell_intersects) | Return true if `cell1` contains `cell2` or `cell2` contains `cell1`.|
| [`s2_cell_level`](#s2_cell_level) | Extract the level (0-30, inclusive) from an S2_CELL.|
| [`s2_cell_parent`](#s2_cell_parent) | Compute a parent S2_CELL.|
| [`s2_cell_range_max`](#s2_cell_range_max) | Compute the maximum leaf cell value contained within an S2_CELL.|
| [`s2_cell_range_min`](#s2_cell_range_min) | Compute the minimum leaf cell value contained within an S2_CELL.|
| [`s2_cell_token`](#s2_cell_token) | Serialize an S2_CELL as a compact hexadecimal token.|
| [`s2_cell_vertex`](#s2_cell_vertex) | Extract a vertex (corner) of an S2 cell.|
| [`s2_cellfromlonlat`](#s2_cellfromlonlat) | Convert a lon/lat pair to S2_CELL_CENTER.|
| [`s2_cellfromwkb`](#s2_cellfromwkb) | Convert a WKB point directly to S2_CELL_CENTER.|
| [`s2_astext`](#s2_astext) | Returns the well-known text (WKT) string of the geography.|
| [`s2_aswkb`](#s2_aswkb) | Serialize a GEOGRAPHY as well-known binary (WKB).|
| [`s2_format`](#s2_format) | Returns the WKT string of the geography with a given precision.|
| [`s2_geogfromtext`](#s2_geogfromtext) | Returns the geography from a WKT string.|
| [`s2_geogfromwkb`](#s2_geogfromwkb) | Converts a WKB blob to a geography.|
| [`s2_prepare`](#s2_prepare) | Prepares a geography for faster predicate and overlay operations.|
| [`s2_data_city`](#s2_data_city) | Get an example city or country from [`s2_data_cities()`](#s2_data_cities)|
| [`s2_data_country`](#s2_data_country) | Get an example city or country from [`s2_data_cities()`](#s2_data_cities)|
| [`s2_difference`](#s2_difference) | Returns the difference of two geographies.|
| [`s2_intersection`](#s2_intersection) | Returns the intersection of two geographies.|
| [`s2_union`](#s2_union) | Returns the union of two geographies.|
| [`s2_contains`](#s2_contains) | Returns true if the first geography contains the second.|
| [`s2_equals`](#s2_equals) | Returns true if the two geographies are equal.|
| [`s2_intersects`](#s2_intersects) | Returns true if the two geographies intersect.|
| [`s2_mayintersect`](#s2_mayintersect) | Returns true if the two geographies may intersect.|

## Accessors

### s2_area

Calculate the area of the geography in square meters.

```sql
DOUBLE s2_area(geog GEOGRAPHY)
```

#### Description

The returned area is in square meters as approximated as the area of the polygon
on a perfect sphere.

For non-polygon geographies, `s2_area()` returns `0.0`.

#### Example

```sql
SELECT s2_area(s2_data_country('Fiji')) AS area;
--┌───────────────────┐
--│       area        │
--│      double       │
--├───────────────────┤
--│ 19353593807.95006 │
--└───────────────────┘

SELECT s2_area('POINT (0 0)'::GEOGRAPHY) AS area;
--┌────────┐
--│  area  │
--│ double │
--├────────┤
--│    0.0 │
--└────────┘
```

### s2_isempty

Returns true if the geography is empty.

```sql
BOOLEAN s2_isempty(geog GEOGRAPHY)
```

#### Example

```sql
SELECT s2_isempty('POINT(0 0)') AS is_empty;
--┌──────────┐
--│ is_empty │
--│ boolean  │
--├──────────┤
--│ false    │
--└──────────┘
```

### s2_length

Calculate the length of the geography in meters.

```sql
DOUBLE s2_length(geog GEOGRAPHY)
```

#### Description

For non-linestring or multilinestring geographies, `s2_length()` returns `0.0`.

#### Example

```sql
SELECT s2_length('POINT (0 0)'::GEOGRAPHY) AS length;
--┌────────┐
--│ length │
--│ double │
--├────────┤
--│    0.0 │
--└────────┘

SELECT s2_length('LINESTRING (0 0, -64 45)'::GEOGRAPHY) AS length;
--┌───────────────────┐
--│      length       │
--│      double       │
--├───────────────────┤
--│ 7999627.260862333 │
--└───────────────────┘

SELECT s2_length(s2_data_country('Canada')) AS length;
--┌────────┐
--│ length │
--│ double │
--├────────┤
--│    0.0 │
--└────────┘
```

### s2_perimeter

Calculate the perimeter of the geography in meters.

```sql
DOUBLE s2_perimeter(geog GEOGRAPHY)
```

#### Description

The returned length is in meters as approximated as the perimeter of the polygon
on a perfect sphere.

For non-polygon geographies, `s2_perimeter()` returns `0.0`. For a  polygon with
more than one ring, this function returns the sum of the perimeter of all
rings.

#### Example

```sql
SELECT s2_perimeter(s2_data_country('Fiji')) AS perimeter;
--┌───────────────────┐
--│     perimeter     │
--│      double       │
--├───────────────────┤
--│ 865355.9056990512 │
--└───────────────────┘

SELECT s2_perimeter('POINT (0 0)'::GEOGRAPHY) AS perimeter;
--┌───────────┐
--│ perimeter │
--│  double   │
--├───────────┤
--│       0.0 │
--└───────────┘
```

### s2_x

Extract the longitude of a point geography.

```sql
DOUBLE s2_x(geog GEOGRAPHY)
```

#### Description

For geographies that are not a single point, `NaN` is returned.

#### Example

```sql
SELECT s2_x('POINT (-64 45)'::GEOGRAPHY);
--┌───────────────────────────────────────────┐
--│ s2_x(CAST('POINT (-64 45)' AS GEOGRAPHY)) │
--│                  double                   │
--├───────────────────────────────────────────┤
--│                                     -64.0 │
--└───────────────────────────────────────────┘
```

### s2_y

Extract the latitude of a point geography.

```sql
DOUBLE s2_y(geog GEOGRAPHY)
```

#### Description

For geographies that are not a single point, `NaN` is returned.

#### Example

```sql
SELECT s2_y('POINT (-64 45)'::GEOGRAPHY);
--┌───────────────────────────────────────────┐
--│ s2_y(CAST('POINT (-64 45)' AS GEOGRAPHY)) │
--│                  double                   │
--├───────────────────────────────────────────┤
--│                         44.99999999999999 │
--└───────────────────────────────────────────┘
```
## Bounds

### s2_bounds_rect

Returns the bounds of the input geography as a box with Cartesian edges.

```sql
BOX_LNGLAT s2_bounds_rect(geog GEOGRAPHY)
```

#### Description

The output xmin may be greater than xmax if the geography crosses the
antimeridian.

#### Example

```sql
SELECT s2_bounds_rect(s2_data_country('Germany')) as rect;
--┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                 rect                                                  │
--│                                              box_lnglat                                               │
--├───────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ {'xmin': 5.988658, 'ymin': 47.30248799999997, 'xmax': 15.016996000000002, 'ymax': 54.983104000000026} │
--└───────────────────────────────────────────────────────────────────────────────────────────────────────┘

SELECT s2_bounds_rect(s2_data_country('Fiji')) as rect;
--┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                               rect                                               │
--│                                            box_lnglat                                            │
--├──────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ {'xmin': 177.28504, 'ymin': -18.28799000000003, 'xmax': -179.79332, 'ymax': -16.020881999999975} │
--└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_box_struct

Return a BOX_LNGLAT storage as a struct(xmin, ymin, xmax, ymax).

```sql
STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE) s2_box_struct(box BOX_LNGLAT)
```

#### Example

```sql
SELECT s2_box_struct(s2_bounds_rect('POINT (0 1)'::GEOGRAPHY)) as rect;
--┌────────────────────────────────────────────────────────────┐
--│                            rect                            │
--│ struct(xmin double, ymin double, xmax double, ymax double) │
--├────────────────────────────────────────────────────────────┤
--│ {'xmin': 0.0, 'ymin': 1.0, 'xmax': 0.0, 'ymax': 1.0}       │
--└────────────────────────────────────────────────────────────┘
```

### s2_box_wkb

Serialize a BOX_LNGLAT as WKB for export.

```sql
BLOB s2_box_wkb(box BOX_LNGLAT)
```

#### Example

```sql
SELECT s2_box_wkb(s2_bounds_rect('POINT (0 1)'::GEOGRAPHY)) as rect;
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                         rect                                                         │
--│                                                         blob                                                         │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ \x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0?\…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_covering

Returns the S2 cell covering of the geography.

```sql
S2_CELL_UNION s2_covering(geog GEOGRAPHY)
```

#### Description

A covering is a deterministic S2_CELL_UNION (i.e., list of S2_CELLs) that
completely covers a geography. This is useful as a compact approximation
of a geography that can be used to select possible candidates for intersection.

Note that an S2_CELL_UNION is a thin wrapper around a LIST of S2_CELL, such
that DuckDB LIST functions can be used to unnest, extract, or otherwise
interact with the result.

See the [Cell Operators](#cellops) section for ways to interact with cells.

#### Example

```sql
SELECT s2_covering(s2_data_country('Germany')) AS covering;
--┌────────────────────────────────────────────────────────────────────────────┐
--│                                  covering                                  │
--│                               s2_cell_union                                │
--├────────────────────────────────────────────────────────────────────────────┤
--│ [2/032003, 2/03201, 2/032322, 2/032323, 2/03302, 2/03303, 2/0331, 2/03320] │
--└────────────────────────────────────────────────────────────────────────────┘

-- Find countries that might contain Berlin
SELECT name as country, cell FROM (
  SELECT name, UNNEST(s2_covering(geog)) as cell
  FROM s2_data_countries()
) WHERE
s2_cell_contains(cell, s2_data_city('Berlin')::S2_CELL_CENTER::S2_CELL);
--┌─────────┬─────────┐
--│ country │  cell   │
--│ varchar │ s2_cell │
--├─────────┼─────────┤
--│ Germany │  2/0331 │
--│ France  │   2/033 │
--└─────────┴─────────┘
```

### s2_covering_fixed_level

Returns the S2 cell covering of the geography with a fixed level.

```sql
S2_CELL_UNION s2_covering_fixed_level(geog GEOGRAPHY, fixed_level INTEGER)
```

#### Description

See `[s2_covering](#s2_covering)` for further detail and examples.

#### Example

```sql
SELECT s2_covering_fixed_level(s2_data_country('Germany'), 3) AS covering;
--┌────────────────┐
--│    covering    │
--│ s2_cell_union  │
--├────────────────┤
--│ [2/032, 2/033] │
--└────────────────┘

SELECT s2_covering_fixed_level(s2_data_country('Germany'), 4) AS covering;
--┌──────────────────────────────────────────┐
--│                 covering                 │
--│              s2_cell_union               │
--├──────────────────────────────────────────┤
--│ [2/0320, 2/0323, 2/0330, 2/0331, 2/0332] │
--└──────────────────────────────────────────┘
```
## Cellops

### s2_arbitrarycellfromwkb

Get an arbitrary S2_CELL_CENTER on or near the input.

```sql
S2_CELL_CENTER s2_arbitrarycellfromwkb(wkb BLOB)
```

#### Description

This function parses the minimum required WKB input to obtain the first
longitude/latitude pair it sees and finds the closest S2_CELL_CENTER. This
is useful for sorting or partitioning of lon/lat input when there is no need
to create a GEOGRAPHY.

Note that longitude/latitude is assumed in the input.

#### Example

```sql
SELECT name, s2_arbitrarycellfromwkb(s2_aswkb(geog)) AS cell
FROM s2_data_cities()
LIMIT 5;
--┌──────────────┬──────────────────────────────────┐
--│     name     │               cell               │
--│   varchar    │          s2_cell_center          │
--├──────────────┼──────────────────────────────────┤
--│ Vatican City │ 0/212113230003023131001102313200 │
--│ San Marino   │ 0/212112131211123020010233101310 │
--│ Vaduz        │ 2/033031212023000232111020023022 │
--│ Lobamba      │ 0/331313212213231033112021020221 │
--│ Luxembourg   │ 2/033022221321121102131101113231 │
--└──────────────┴──────────────────────────────────┘

-- Use to partition arbitrary lon/lat input
COPY (
  SELECT
    geog.s2_aswkb().s2_arbitrarycellfromwkb().s2_cell_parent(2).s2_cell_token() AS partition_cell,
    name,
    geog.s2_aswkb()
  FROM s2_data_cities()
) TO 'cities' WITH (FORMAT PARQUET, PARTITION_BY partition_cell);

SELECT * FROM glob('cities/**') LIMIT 5;
--┌─────────────────────────────────────────┐
--│                  file                   │
--│                 varchar                 │
--├─────────────────────────────────────────┤
--│ cities/partition_cell=01/data_0.parquet │
--│ cities/partition_cell=09/data_0.parquet │
--│ cities/partition_cell=0d/data_0.parquet │
--│ cities/partition_cell=0f/data_0.parquet │
--│ cities/partition_cell=11/data_0.parquet │
--└─────────────────────────────────────────┘
```

### s2_cell_child

Compute a child S2_CELL.

```sql
S2_CELL s2_cell_child(cell S2_CELL, index INTEGER)
```

#### Description

Each S2_CELL that is not a leaf cell (level 30) has exactly four children
(index 0-3 inclusive). Values for `index` outside this range will result in
an invalid returned cell.

#### Example

```sql
SELECT s2_cell_child('5/00000'::S2_CELL, ind) as cell
FROM (VALUES (0), (1), (2), (3), (4)) indices(ind);
--┌───────────────────────────┐
--│           cell            │
--│          s2_cell          │
--├───────────────────────────┤
--│                  5/000000 │
--│                  5/000001 │
--│                  5/000002 │
--│                  5/000003 │
--│ Invalid: ffffffffffffffff │
--└───────────────────────────┘
```

### s2_cell_contains

Return true if `cell1` contains `cell2`.

```sql
BOOLEAN s2_cell_contains(cell1 S2_CELL, cell2 S2_CELL)
```

#### Description

See [`s2_cell_range_min()`](#s2_cell_range_min) and [`s2_cell_range_max()`](#s2_cell_range_max)
for how to calculate this in a way that DuckDB can use to accelerate a join.

#### Example

```sql
SELECT s2_cell_contains('5/3'::S2_CELL, '5/30'::S2_CELL) AS result;
--┌─────────┐
--│ result  │
--│ boolean │
--├─────────┤
--│ true    │
--└─────────┘

SELECT s2_cell_contains('5/30'::S2_CELL, '5/3'::S2_CELL) AS result;
--┌─────────┐
--│ result  │
--│ boolean │
--├─────────┤
--│ false   │
--└─────────┘
```

### s2_cell_edge_neighbor

Compute a neighbor S2_CELL.

```sql
S2_CELL s2_cell_edge_neighbor(cell S2_CELL, index INTEGER)
```

#### Description

Every S2_CELL has a neighbor at the top, left, right, and bottom,
which can be selected from index values 0-3 (inclusive). Values of
`index` outside this range will result in an invalid returned cell value.

#### Example

```sql
SELECT s2_cell_edge_neighbor('5/00000'::S2_CELL, ind) as cell
FROM (VALUES (0), (1), (2), (3), (4)) indices(ind);
--┌───────────────────────────┐
--│           cell            │
--│          s2_cell          │
--├───────────────────────────┤
--│                   3/22222 │
--│                   5/00001 │
--│                   5/00003 │
--│                   4/33333 │
--│ Invalid: ffffffffffffffff │
--└───────────────────────────┘
```

### s2_cell_from_token

Parse a hexadecimal token as an S2_CELL.

```sql
S2_CELL s2_cell_from_token(text VARCHAR)
```

#### Description

Note that invalid strings are given an invalid cell value of 0 but do not error.
To parse the more user-friendly debug string format, cast from `VARCHAR` to
`S2_CELL`.

#### Example

```sql
SELECT s2_cell_from_token('4b59a0cd83b5de49');
--┌────────────────────────────────────────┐
--│ s2_cell_from_token('4b59a0cd83b5de49') │
--│                s2_cell                 │
--├────────────────────────────────────────┤
--│       2/112230310012123001312232330210 │
--└────────────────────────────────────────┘

-- Invalid strings don't error but do parse into an invalid cell id
SELECT s2_cell_from_token('foofy');
--┌─────────────────────────────┐
--│ s2_cell_from_token('foofy') │
--│           s2_cell           │
--├─────────────────────────────┤
--│   Invalid: 0000000000000000 │
--└─────────────────────────────┘
```

### s2_cell_intersects

Return true if `cell1` contains `cell2` or `cell2` contains `cell1`.

```sql
BOOLEAN s2_cell_intersects(cell1 S2_CELL, cell2 S2_CELL)
```

#### Description

See [`s2_cell_range_min()`](#s2_cell_range_min) and [`s2_cell_range_max()`](#s2_cell_range_max)
for how to calculate this in a way that DuckDB can use to accelerate a join.

Note that this will return false for neighboring cells. Use [`s2_intersects()`](#s2_intersects)
if you need this type of intersection check.

#### Example

```sql
SELECT s2_cell_intersects('5/3'::S2_CELL, '5/30'::S2_CELL) AS result;
--┌─────────┐
--│ result  │
--│ boolean │
--├─────────┤
--│ true    │
--└─────────┘

SELECT s2_cell_intersects('5/30'::S2_CELL, '5/3'::S2_CELL) AS result;
--┌─────────┐
--│ result  │
--│ boolean │
--├─────────┤
--│ true    │
--└─────────┘
```

### s2_cell_level

Extract the level (0-30, inclusive) from an S2_CELL.

```sql
TINYINT s2_cell_level(cell S2_CELL)
```

#### Example

```sql
SELECT s2_cell_level('5/33120'::S2_CELL);
--┌───────────────────────────────────────────┐
--│ s2_cell_level(CAST('5/33120' AS S2_CELL)) │
--│                   int8                    │
--├───────────────────────────────────────────┤
--│                                         5 │
--└───────────────────────────────────────────┘
```

### s2_cell_parent

Compute a parent S2_CELL.

```sql
S2_CELL s2_cell_parent(cell S2_CELL, level INTEGER)
```

#### Description

Note that level is clamped to the valid range 0-30. A negative value will
be subtracted from the current level (e.g., use `-1` for the immediate parent).

#### Example

```sql
SELECT s2_cell_parent(s2_cellfromlonlat(-64, 45), level) as cell
FROM (VALUES (0), (1), (2), (3), (4), (5), (-1), (-2)) levels(level);
--┌─────────────────────────────────┐
--│              cell               │
--│             s2_cell             │
--├─────────────────────────────────┤
--│                              2/ │
--│                             2/1 │
--│                            2/11 │
--│                           2/112 │
--│                          2/1122 │
--│                         2/11223 │
--│ 2/11223031001212300131223233021 │
--│  2/1122303100121230013122323302 │
--└─────────────────────────────────┘
```

### s2_cell_range_max

Compute the maximum leaf cell value contained within an S2_CELL.

```sql
S2_CELL s2_cell_range_max(cell S2_CELL)
```

#### Example

```sql
SELECT
  s2_cell_range_min('5/00000'::S2_CELL) AS cell_min,
  s2_cell_range_max('5/00000'::S2_CELL) AS cell_max;
--┌──────────────────────────────────┬──────────────────────────────────┐
--│             cell_min             │             cell_max             │
--│             s2_cell              │             s2_cell              │
--├──────────────────────────────────┼──────────────────────────────────┤
--│ 5/000000000000000000000000000000 │ 5/000003333333333333333333333333 │
--└──────────────────────────────────┴──────────────────────────────────┘
```

### s2_cell_range_min

Compute the minimum leaf cell value contained within an S2_CELL.

```sql
S2_CELL s2_cell_range_min(cell S2_CELL)
```

#### Example

```sql
SELECT
  s2_cell_range_min('5/00000'::S2_CELL) AS cell_min,
  s2_cell_range_max('5/00000'::S2_CELL) AS cell_max;
--┌──────────────────────────────────┬──────────────────────────────────┐
--│             cell_min             │             cell_max             │
--│             s2_cell              │             s2_cell              │
--├──────────────────────────────────┼──────────────────────────────────┤
--│ 5/000000000000000000000000000000 │ 5/000003333333333333333333333333 │
--└──────────────────────────────────┴──────────────────────────────────┘
```

### s2_cell_token

Serialize an S2_CELL as a compact hexadecimal token.

```sql
VARCHAR s2_cell_token(cell S2_CELL)
```

#### Description

To serialize to a more user-friendly (but longer) string, cast an `S2_CELL`
to `VARCHAR`.

#### Example

```sql
SELECT s2_cell_token(s2_cellfromlonlat(-64, 45));
--┌───────────────────────────────────────────┐
--│ s2_cell_token(s2_cellfromlonlat(-64, 45)) │
--│                  varchar                  │
--├───────────────────────────────────────────┤
--│ 4b59a0cd83b5de49                          │
--└───────────────────────────────────────────┘

SELECT s2_cell_token('5/3301'::S2_CELL);
--┌──────────────────────────────────────────┐
--│ s2_cell_token(CAST('5/3301' AS S2_CELL)) │
--│                 varchar                  │
--├──────────────────────────────────────────┤
--│ be3                                      │
--└──────────────────────────────────────────┘
```

### s2_cell_vertex

Extract a vertex (corner) of an S2 cell.

```sql
GEOGRAPHY s2_cell_vertex(cell_id S2_CELL, vertex_id INTEGER)
```

#### Description

An S2_CELL is represented by an unsigned 64-bit integer but logically
represents a polygon with four vertices. This function extracts one of them
according to `vertex_id` (an integer from 0-3).

It is usually more convenient to cast an S2_CELL to GEOGRAPHY or pass an
S2_CELL directly to a function that accepts a GEOGRAPHY an use the implicit
conversion.

#### Example

```sql
SELECT s2_cell_vertex('5/'::S2_CELL, id) as vertex,
FROM (VALUES (0), (1), (2), (3)) vertices(id);
--┌──────────────────────────────────┐
--│              vertex              │
--│            geography             │
--├──────────────────────────────────┤
--│ POINT (-135 -35.264389682754654) │
--│ POINT (135 -35.264389682754654)  │
--│ POINT (45 -35.264389682754654)   │
--│ POINT (-45 -35.264389682754654)  │
--└──────────────────────────────────┘

-- Usually easier to cast to GEOGRAPHY
SELECT '5/'::S2_CELL::GEOGRAPHY as geog;
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                         geog                                                         │
--│                                                      geography                                                       │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ POLYGON ((-135 -35.264389682754654, -225 -35.264389682754654, -315 -35.264389682754654, -405 -35.264389682754654, …  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_cellfromlonlat

Convert a lon/lat pair to S2_CELL_CENTER.

```sql
S2_CELL_CENTER s2_cellfromlonlat(lon DOUBLE, lat DOUBLE)
```

#### Description

Cell centers are a highly efficient type for storing point data where a
precision loss of up to ~2cm is acceptable.

See [`s2_x()`](#s2_x) and [`s2_y()`](#s2_y) for the reverse operation.

#### Example

```sql
SELECT s2_cellfromlonlat(-64, 45);
--┌──────────────────────────────────┐
--│    s2_cellfromlonlat(-64, 45)    │
--│          s2_cell_center          │
--├──────────────────────────────────┤
--│ 2/112230310012123001312232330210 │
--└──────────────────────────────────┘

SELECT name, s2_cellfromlonlat(s2_x(geog), s2_y(geog)) as cell
FROM s2_data_cities()
LIMIT 5;
--┌──────────────┬──────────────────────────────────┐
--│     name     │               cell               │
--│   varchar    │          s2_cell_center          │
--├──────────────┼──────────────────────────────────┤
--│ Vatican City │ 0/212113230003023131001102313200 │
--│ San Marino   │ 0/212112131211123020010233101310 │
--│ Vaduz        │ 2/033031212023000232111020023022 │
--│ Lobamba      │ 0/331313212213231033112021020221 │
--│ Luxembourg   │ 2/033022221321121102131101113231 │
--└──────────────┴──────────────────────────────────┘
```

### s2_cellfromwkb

Convert a WKB point directly to S2_CELL_CENTER.

```sql
S2_CELL_CENTER s2_cellfromwkb(wkb BLOB)
```

#### Description

This is the same as `s2_geogfromwkb()::S2_CELL_CENTER` but does the parsing
directly to maximize performance. Cell centers are a highly efficient type
for storing point data where a precision loss of up to ~2cm is acceptable;
this function exists to ensure getting data into this format is as easy as
possible.

This function assumes the input WKB contains longitude/latitude coordinates
and will error for any input that is not a POINT or MULTIPOINT with exactly
one point.

#### Example

```sql
SELECT name, s2_cellfromwkb(s2_aswkb(geog)) as cell
FROM s2_data_cities()
LIMIT 5;
--┌──────────────┬──────────────────────────────────┐
--│     name     │               cell               │
--│   varchar    │          s2_cell_center          │
--├──────────────┼──────────────────────────────────┤
--│ Vatican City │ 0/212113230003023131001102313200 │
--│ San Marino   │ 0/212112131211123020010233101310 │
--│ Vaduz        │ 2/033031212023000232111020023022 │
--│ Lobamba      │ 0/331313212213231033112021020221 │
--│ Luxembourg   │ 2/033022221321121102131101113231 │
--└──────────────┴──────────────────────────────────┘
```
## Conversion

### s2_astext

Returns the well-known text (WKT) string of the geography.

```sql
VARCHAR s2_astext(geog GEOGRAPHY)
```

#### Description

Note that because the internal representation of the GEOGRAPHY type is either
an S2_CELL_CENTER or a unit vector, WKT typically does not roundtrip through a
GEOGRAPHY unless the output is rounded using `[s2_format()`][#s2_format].

The output contains spherical edges. If edges are large and the consumer does
not know that the edges are spherical, this may cause issues.

Calling this function has the same effect as casting to VARCHAR.

#### Example

```sql
SELECT s2_astext(s2_data_city('Vancouver'));
--┌──────────────────────────────────────┐
--│ s2_astext(s2_data_city('Vancouver')) │
--│               varchar                │
--├──────────────────────────────────────┤
--│ POINT (-123.12359 49.2753624)        │
--└──────────────────────────────────────┘
```

### s2_aswkb

Serialize a GEOGRAPHY as well-known binary (WKB).

```sql
BLOB s2_aswkb(geog GEOGRAPHY)
```

#### Description

Note that because the internal representation of the GEOGRAPHY type is either
an S2_CELL_CENTER or a unit vector, WKB typically does not roundtrip through a
GEOGRAPHY.

The output contains spherical edges. If edges are large and the consumer does
not know that the edges are spherical, this may cause issues.

#### Example

```sql
SELECT s2_aswkb(s2_data_city('Toronto')) as wkb;
--┌───────────────────────────────────────────────────────────────────────┐
--│                                  wkb                                  │
--│                                 blob                                  │
--├───────────────────────────────────────────────────────────────────────┤
--│ \x01\x01\x00\x00\x00\x11 \x9E\x80\x01\xDBS\xC0g\xDC\x8A\xB3\xD8\xD9E@ │
--└───────────────────────────────────────────────────────────────────────┘
```

### s2_format

Returns the WKT string of the geography with a given precision.

```sql
VARCHAR s2_format(geog GEOGRAPHY, precision TINYINT)
```

#### Description

See [`s2_astext()`](#s2_text) for parameter-free lossless output. Like `s2_text()`,
this function exports spherical edges.

#### Example

```sql
SELECT s2_format(s2_data_city('Vancouver'), 1);
--┌─────────────────────────────────────────┐
--│ s2_format(s2_data_city('Vancouver'), 1) │
--│                 varchar                 │
--├─────────────────────────────────────────┤
--│ POINT (-123.1 49.3)                     │
--└─────────────────────────────────────────┘
```

### s2_geogfromtext

Returns the geography from a WKT string.

```sql
GEOGRAPHY s2_geogfromtext(wkt VARCHAR)
```

### s2_geogfromwkb

Converts a WKB blob to a geography.

```sql
GEOGRAPHY s2_geogfromwkb(wkb BLOB)
```

#### Description

The input WKB blog is assumed to have longitude/latitude coordinates and have
spherical edges. If edges are long and the input had a different edge type,
the resulting GEOGRAPHY may be invalid or represent a different location than
intended.

#### Example

```sql
SELECT s2_geogfromwkb(s2_aswkb(s2_data_city('Toronto'))) as geog;
--┌────────────────────────────────┐
--│              geog              │
--│           geography            │
--├────────────────────────────────┤
--│ POINT (-79.4219667 43.7019257) │
--└────────────────────────────────┘
```

### s2_prepare

Prepares a geography for faster predicate and overlay operations.

```sql
GEOGRAPHY s2_prepare(geog GEOGRAPHY)
```

#### Description

For advanced users, this is useful for preparing input that will be subject
to a large number of intersection or containment checks. This high level terms,
this operation builds a cell-based index on the edges of the geography that
would otherwise have to occur on every intersection check.

This function returns its input for very small geographies (e.g., points)
that do not benefit from this operation.

#### Example

```sql
SELECT s2_prepare(s2_data_country('Fiji'));
--┌─────────────────────────────────────┐
--│ s2_prepare(s2_data_country('Fiji')) │
--│              geography              │
--├─────────────────────────────────────┤
--│ <S2ShapeIndex 478 b>                │
--└─────────────────────────────────────┘

CREATE TABLE countries AS
SELECT name, s2_prepare(geog) as geog
FROM s2_data_countries();

SELECT cities.name as city, countries.name as country
FROM s2_data_cities() AS cities
INNER JOIN countries ON s2_contains(countries.geog, cities.geog)
LIMIT 5;
--┌───────────┬──────────────────────┐
--│   city    │       country        │
--│  varchar  │       varchar        │
--├───────────┼──────────────────────┤
--│ Kabul     │ Afghanistan          │
--│ Luanda    │ Angola               │
--│ Tirana    │ Albania              │
--│ Abu Dhabi │ United Arab Emirates │
--│ Dubai     │ United Arab Emirates │
--└───────────┴──────────────────────┘
```
## Data

### s2_data_city

Get an example city or country from [`s2_data_cities()`](#s2_data_cities)

```sql
GEOGRAPHY s2_data_city(name VARCHAR)
```

#### Description

or [`s2_data_countries()`](#s2_data_countries) by name.

#### Example

```sql
SELECT s2_data_city('Toronto') as city;
--┌────────────────────────────────┐
--│              city              │
--│           geography            │
--├────────────────────────────────┤
--│ POINT (-79.4219667 43.7019257) │
--└────────────────────────────────┘

SELECT s2_data_country('Fiji') as country;
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                       country                                                        │
--│                                                      geography                                                       │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ MULTIPOLYGON (((178.3736 -17.33992, 178.12557 -17.50481, 177.67087 -17.381139999999995, 177.28504 -17.72465, 177.3…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_data_country

Get an example city or country from [`s2_data_cities()`](#s2_data_cities)

```sql
GEOGRAPHY s2_data_country(name VARCHAR)
```

#### Description

or [`s2_data_countries()`](#s2_data_countries) by name.

#### Example

```sql
SELECT s2_data_city('Toronto') as city;
--┌────────────────────────────────┐
--│              city              │
--│           geography            │
--├────────────────────────────────┤
--│ POINT (-79.4219667 43.7019257) │
--└────────────────────────────────┘

SELECT s2_data_country('Fiji') as country;
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                       country                                                        │
--│                                                      geography                                                       │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ MULTIPOLYGON (((178.3736 -17.33992, 178.12557 -17.50481, 177.67087 -17.381139999999995, 177.28504 -17.72465, 177.3…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
## Overlay

### s2_difference

Returns the difference of two geographies.

```sql
GEOGRAPHY s2_difference(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Example

```sql
SELECT s2_difference(
  'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))'
) as difference
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                      difference                                                      │
--│                                                      geography                                                       │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ POLYGON ((5.000000000000001 10.037423045910714, 0 10, 0 0, 10 0, 9.999999999999998 5.019001817489642, 4.9999999999…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_intersection

Returns the intersection of two geographies.

```sql
GEOGRAPHY s2_intersection(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Example

```sql
SELECT s2_intersection(
  'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))'
) as intersection
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                     intersection                                                     │
--│                                                      geography                                                       │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ POLYGON ((4.999999999999999 4.999999999999999, 9.999999999999998 5.019001817489642, 10 10.000000000000002, 5.00000…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_union

Returns the union of two geographies.

```sql
GEOGRAPHY s2_union(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Example

```sql
SELECT s2_union(
  'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))',
  'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))'
) as union_
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                        union_                                                        │
--│                                                      geography                                                       │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ POLYGON ((5.000000000000001 10.037423045910714, 0 10, 0 0, 10 0, 9.999999999999998 5.019001817489642, 14.999999999…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
## Predicates

### s2_contains

Returns true if the first geography contains the second.

```sql
BOOLEAN s2_contains(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Example

```sql
SELECT s2_contains(s2_data_country('Canada'), s2_data_city('Toronto'));
--┌─────────────────────────────────────────────────────────────────┐
--│ s2_contains(s2_data_country('Canada'), s2_data_city('Toronto')) │
--│                             boolean                             │
--├─────────────────────────────────────────────────────────────────┤
--│ true                                                            │
--└─────────────────────────────────────────────────────────────────┘

SELECT s2_contains(s2_data_city('Toronto'), s2_data_country('Canada'));
--┌─────────────────────────────────────────────────────────────────┐
--│ s2_contains(s2_data_city('Toronto'), s2_data_country('Canada')) │
--│                             boolean                             │
--├─────────────────────────────────────────────────────────────────┤
--│ false                                                           │
--└─────────────────────────────────────────────────────────────────┘

SELECT s2_contains(s2_data_country('Canada'), s2_data_city('Chicago'));
--┌─────────────────────────────────────────────────────────────────┐
--│ s2_contains(s2_data_country('Canada'), s2_data_city('Chicago')) │
--│                             boolean                             │
--├─────────────────────────────────────────────────────────────────┤
--│ false                                                           │
--└─────────────────────────────────────────────────────────────────┘
```

### s2_equals

Returns true if the two geographies are equal.

```sql
BOOLEAN s2_equals(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Description

Note that this test of equality will pass for *geometrically* equal geographies
that may have the same edges but that are ordered differently.

#### Example

```sql
SELECT s2_equals(s2_data_country('Canada'), s2_data_country('Canada'));
--┌─────────────────────────────────────────────────────────────────┐
--│ s2_equals(s2_data_country('Canada'), s2_data_country('Canada')) │
--│                             boolean                             │
--├─────────────────────────────────────────────────────────────────┤
--│ true                                                            │
--└─────────────────────────────────────────────────────────────────┘

SELECT s2_equals(s2_data_city('Toronto'), s2_data_country('Canada'));
--┌───────────────────────────────────────────────────────────────┐
--│ s2_equals(s2_data_city('Toronto'), s2_data_country('Canada')) │
--│                            boolean                            │
--├───────────────────────────────────────────────────────────────┤
--│ false                                                         │
--└───────────────────────────────────────────────────────────────┘
```

### s2_intersects

Returns true if the two geographies intersect.

```sql
BOOLEAN s2_intersects(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Example

```sql
SELECT s2_intersects(s2_data_country('Canada'), s2_data_city('Toronto'));
--┌───────────────────────────────────────────────────────────────────┐
--│ s2_intersects(s2_data_country('Canada'), s2_data_city('Toronto')) │
--│                              boolean                              │
--├───────────────────────────────────────────────────────────────────┤
--│ true                                                              │
--└───────────────────────────────────────────────────────────────────┘

SELECT s2_intersects(s2_data_country('Canada'), s2_data_city('Chicago'));
--┌───────────────────────────────────────────────────────────────────┐
--│ s2_intersects(s2_data_country('Canada'), s2_data_city('Chicago')) │
--│                              boolean                              │
--├───────────────────────────────────────────────────────────────────┤
--│ false                                                             │
--└───────────────────────────────────────────────────────────────────┘
```

### s2_mayintersect

Returns true if the two geographies may intersect.

```sql
BOOLEAN s2_mayintersect(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

#### Description

This function uses the internal [covering](#s2_covering) stored alongside
each geography to perform a cheap check for potential intersection.

#### Example

```sql
-- Definitely intersects
SELECT s2_mayintersect(s2_data_country('Canada'), s2_data_city('Toronto'));
--┌─────────────────────────────────────────────────────────────────────┐
--│ s2_mayintersect(s2_data_country('Canada'), s2_data_city('Toronto')) │
--│                               boolean                               │
--├─────────────────────────────────────────────────────────────────────┤
--│ true                                                                │
--└─────────────────────────────────────────────────────────────────────┘

-- Doesn't intersect but might according to the internal coverings
SELECT s2_mayintersect(s2_data_country('Canada'), s2_data_city('Chicago'));
--┌─────────────────────────────────────────────────────────────────────┐
--│ s2_mayintersect(s2_data_country('Canada'), s2_data_city('Chicago')) │
--│                               boolean                               │
--├─────────────────────────────────────────────────────────────────────┤
--│ true                                                                │
--└─────────────────────────────────────────────────────────────────────┘

-- Definitely doesn't intersect
SELECT s2_mayintersect(s2_data_country('Canada'), s2_data_city('Berlin'));
--┌────────────────────────────────────────────────────────────────────┐
--│ s2_mayintersect(s2_data_country('Canada'), s2_data_city('Berlin')) │
--│                              boolean                               │
--├────────────────────────────────────────────────────────────────────┤
--│ true                                                               │
--└────────────────────────────────────────────────────────────────────┘
```
