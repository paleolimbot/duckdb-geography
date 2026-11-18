
# Function Reference

| Function | Summary |
| --- | --- |
| [`s2_area`](#s2_area) | Returns the area of the geography.|
| [`s2_isempty`](#s2_isempty) | Returns true if the geography is empty.|
| [`s2_length`](#s2_length) | Returns the length of the geography.|
| [`s2_perimeter`](#s2_perimeter) | Returns the perimeter of the geography.|
| [`s2_x`](#s2_x) | Returns the x coordinate of the geography.|
| [`s2_y`](#s2_y) | Returns the y coordinate of the geography.|
| [`s2_covering`](#s2_covering) | Returns the S2 cell covering of the geography.|
| [`s2_covering_fixed_level`](#s2_covering_fixed_level) | Returns the S2 cell covering of the geography with a fixed level.|
| [`s2_arbitrarycellfromwkb`](#s2_arbitrarycellfromwkb) | Convert the first vertex to S2_CELL_CENTER for sorting.|
| [`s2_cell_child`](#s2_cell_child) | |
| [`s2_cell_contains`](#s2_cell_contains) | |
| [`s2_cell_edge_neighbor`](#s2_cell_edge_neighbor) | |
| [`s2_cell_from_token`](#s2_cell_from_token) | |
| [`s2_cell_intersects`](#s2_cell_intersects) | |
| [`s2_cell_level`](#s2_cell_level) | |
| [`s2_cell_parent`](#s2_cell_parent) | |
| [`s2_cell_range_max`](#s2_cell_range_max) | |
| [`s2_cell_range_min`](#s2_cell_range_min) | |
| [`s2_cell_token`](#s2_cell_token) | |
| [`s2_cell_vertex`](#s2_cell_vertex) | Returns the vertex of the S2 cell.|
| [`s2_cellfromlonlat`](#s2_cellfromlonlat) | Convert a lon/lat pair to S2_CELL_CENTER|
| [`s2_cellfromwkb`](#s2_cellfromwkb) | Convert a WKB point directly to S2_CELL_CENTER|
| [`s2_astext`](#s2_astext) | Returns the WKT string of the geography.|
| [`s2_aswkb`](#s2_aswkb) | Returns the WKB blob of the geography.|
| [`s2_format`](#s2_format) | Returns the WKT string of the geography with a given precision.|
| [`s2_geogfromtext`](#s2_geogfromtext) | Returns the geography from a WKT string.|
| [`s2_geogfromwkb`](#s2_geogfromwkb) | Converts a WKB blob to a geography.|
| [`s2_data_city`](#s2_data_city) | |
| [`s2_data_country`](#s2_data_country) | |
| [`s2_difference`](#s2_difference) | Returns the difference of two geographies.|
| [`s2_intersection`](#s2_intersection) | Returns the intersection of two geographies.|
| [`s2_union`](#s2_union) | Returns the union of two geographies.|
| [`s2_contains`](#s2_contains) | Returns true if the first geography contains the second.|
| [`s2_disjoint`](#s2_disjoint) | Returns true if the two geographies do not intersect.|
| [`s2_equals`](#s2_equals) | Returns true if the two geographies are equal.|
| [`s2_intersects`](#s2_intersects) | Returns true if the two geographies intersect.|
| [`s2_mayintersect`](#s2_mayintersect) | Returns true if the two geographies may intersect.|
| [`s2_within`](#s2_within) | Returns true if the first geography is within the second.|


## Accessors


### s2_area

Returns the area of the geography.

```sql
DOUBLE s2_area(geog GEOGRAPHY)
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

Returns the length of the geography.

```sql
DOUBLE s2_length(geog GEOGRAPHY)
```


### s2_perimeter

Returns the perimeter of the geography.

```sql
DOUBLE s2_perimeter(geog GEOGRAPHY)
```


### s2_x

Returns the x coordinate of the geography.

```sql
DOUBLE s2_x(geog GEOGRAPHY)
```


### s2_y

Returns the y coordinate of the geography.

```sql
DOUBLE s2_y(geog GEOGRAPHY)
```

## Bounds


### s2_covering

Returns the S2 cell covering of the geography.

```sql
S2_CELL_UNION s2_covering(geog GEOGRAPHY)
```


#### Example

```sql
SELECT s2_covering('POINT(0 0)') AS covering;
--┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--│                                                       covering                                                       │
--│                                                    s2_cell_union                                                     │
--├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
--│ [0/022222222222222222222222222222, 0/133333333333333333333333333333, 0/200000000000000000000000000000, 0/311111111…  │
--└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### s2_covering_fixed_level

Returns the S2 cell covering of the geography with a fixed level.

```sql
S2_CELL_UNION s2_covering_fixed_level(geog GEOGRAPHY, fixed_level INTEGER)
```


#### Example

```sql
SELECT s2_covering_fixed_level('POINT(0 0)', 4) AS covering;
--┌──────────────────────────────────┐
--│             covering             │
--│          s2_cell_union           │
--├──────────────────────────────────┤
--│ [0/0222, 0/1333, 0/2000, 0/3111] │
--└──────────────────────────────────┘
```
## Cellops


### s2_arbitrarycellfromwkb

Convert the first vertex to S2_CELL_CENTER for sorting.

```sql
S2_CELL_CENTER s2_arbitrarycellfromwkb(wkb BLOB)
```


### s2_cell_child



```sql
S2_CELL s2_cell_child(cell S2_CELL, index TINYINT)
```


### s2_cell_contains



```sql
BOOLEAN s2_cell_contains(cell1 S2_CELL, cell2 S2_CELL)
```


### s2_cell_edge_neighbor



```sql
S2_CELL s2_cell_edge_neighbor(cell S2_CELL, index TINYINT)
```


### s2_cell_from_token



```sql
S2_CELL s2_cell_from_token(text VARCHAR)
```


### s2_cell_intersects



```sql
BOOLEAN s2_cell_intersects(cell1 S2_CELL, cell2 S2_CELL)
```


### s2_cell_level



```sql
TINYINT s2_cell_level(cell S2_CELL)
```


### s2_cell_parent



```sql
S2_CELL s2_cell_parent(cell S2_CELL, index TINYINT)
```


### s2_cell_range_max



```sql
S2_CELL s2_cell_range_max(cell S2_CELL)
```


### s2_cell_range_min



```sql
S2_CELL s2_cell_range_min(cell S2_CELL)
```


### s2_cell_token



```sql
VARCHAR s2_cell_token(cell S2_CELL)
```


### s2_cell_vertex

Returns the vertex of the S2 cell.

```sql
GEOGRAPHY s2_cell_vertex(cell_id S2_CELL, vertex_id TINYINT)
```


### s2_cellfromlonlat

Convert a lon/lat pair to S2_CELL_CENTER

```sql
S2_CELL_CENTER s2_cellfromlonlat(lon DOUBLE, lat DOUBLE)
```


### s2_cellfromwkb

Convert a WKB point directly to S2_CELL_CENTER

```sql
S2_CELL_CENTER s2_cellfromwkb(wkb BLOB)
```

## Conversion


### s2_astext

Returns the WKT string of the geography.

```sql
VARCHAR s2_astext(geog GEOGRAPHY)
```


### s2_aswkb

Returns the WKB blob of the geography.

```sql
BLOB s2_aswkb(geog GEOGRAPHY)
```


### s2_format

Returns the WKT string of the geography with a given precision.

```sql
VARCHAR s2_format(geog GEOGRAPHY, precision TINYINT)
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


## Data


### s2_data_city



```sql
GEOGRAPHY s2_data_city(name VARCHAR)
```


### s2_data_country



```sql
GEOGRAPHY s2_data_country(name VARCHAR)
```

## Overlay


### s2_difference

Returns the difference of two geographies.

```sql
GEOGRAPHY s2_difference(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_intersection

Returns the intersection of two geographies.

```sql
GEOGRAPHY s2_intersection(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_union

Returns the union of two geographies.

```sql
GEOGRAPHY s2_union(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```

## Predicate


### s2_contains

Returns true if the first geography contains the second.

```sql
BOOLEAN s2_contains(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_disjoint

Returns true if the two geographies do not intersect.

```sql
BOOLEAN s2_disjoint(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_equals

Returns true if the two geographies are equal.

```sql
BOOLEAN s2_equals(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_intersects

Returns true if the two geographies intersect.

```sql
BOOLEAN s2_intersects(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_mayintersect

Returns true if the two geographies may intersect.

```sql
BOOLEAN s2_mayintersect(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```


### s2_within

Returns true if the first geography is within the second.

```sql
BOOLEAN s2_within(geog1 GEOGRAPHY, geog2 GEOGRAPHY)
```
