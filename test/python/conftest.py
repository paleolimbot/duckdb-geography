import glob
from pathlib import Path

import pytest
import duckdb
import warnings


HERE = Path(__file__).parent


def _install_dev_and_connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": True})

    possible_builds = glob.glob(
        "build/**/geography/geography.duckdb_extension",
        recursive=True,
        root_dir=HERE.parent.parent,
    )
    if possible_builds:
        con.install_extension(possible_builds[0], force_install=True)
    else:
        warnings.warn(
            "Can't find build directory for geography.duckdb_extension; skipping INSTALL"
        )

    con.load_extension("geography")
    return con


@pytest.fixture()
def geoarrow_con():
    con = _install_dev_and_connect()
    con.sql("""CALL s2_register_geoarrow_extensions()""")
    return con


@pytest.fixture()
def con():
    return _install_dev_and_connect()
