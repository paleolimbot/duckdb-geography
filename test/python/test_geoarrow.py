from pathlib import Path
import json

import pyarrow as pa
import pytest

import duckdb

HERE = Path(__file__).parent


def test_export_without_register(con):
    tab = con.sql("""SELECT s2_geogfromtext('POINT (0 1)') as geom;""").to_arrow_table()
    assert tab.schema.field("geom").metadata is None


def test_basic_export(geoarrow_con):
    tab = geoarrow_con.sql(
        """SELECT s2_geogfromtext('POINT (0 1)') as geom;"""
    ).to_arrow_table()
    metadata = tab.schema.field("geom").metadata
    assert metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
    params = json.loads(metadata[b"ARROW:extension:metadata"])
    assert params["edges"] == "spherical"
    assert params["crs"] == "OGC:CRS84"


def test_basic_import(geoarrow_con):
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={
            "ARROW:extension:name": "geoarrow.wkb",
            "ARROW:extension:metadata": '{"edges": "spherical"}',
        },
    )
    point_wkb = (
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@"
    )
    schema = pa.schema([field])
    geo_table = pa.table(
        [pa.array([point_wkb])],
        schema=schema,
    )

    tab = geoarrow_con.sql(
        """SELECT s2_astext(geometry) as wkt FROM geo_table;"""
    ).to_arrow_table()
    assert tab["wkt"].to_pylist() == ["POINT (10 20)"]


def test_reject_planar_edges(geoarrow_con):
    # Empty metadata
    bad_metadata = {
        "ARROW:extension:name": "geoarrow.wkb",
        "ARROW:extension:metadata": "",
    }
    field = pa.field("geometry", pa.binary(), metadata=bad_metadata)
    geo_table = pa.table([pa.array([], pa.binary())], schema=pa.schema([field]))
    with pytest.raises(
        duckdb.NotImplementedException,
        match="Can't import non-spherical edges as GEOGRAPHY",
    ):
        geoarrow_con.sql("""SELECT * from geo_table""")


def test_roundtrip_segments(geoarrow_con):
    countries_file = HERE.parent.parent / "data" / "countries.tsv"
    geo_table = geoarrow_con.sql(
        f"""SELECT s2_geogfromtext(geog) as geog1 FROM '{countries_file}'"""
    ).to_arrow_table()
    geo_table2 = geoarrow_con.sql(
        """SELECT geog1 as geog2 FROM geo_table"""
    ).to_arrow_table()

    table_both = pa.table(
        [geo_table["geog1"], geo_table2["geog2"]],
        schema=pa.schema([geo_table.schema.field(0), geo_table2.schema.field(0)]),
    )
    areas_equal = geoarrow_con.sql(
        """SELECT sum(abs(s2_area(geog1) - s2_area(geog2)) < 0.1)::BIGINT AS sum_eq FROM table_both"""
    ).to_arrow_table()
    assert areas_equal == pa.table({"sum_eq": [len(table_both)]})
