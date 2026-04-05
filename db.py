import click
import duckdb
from pathlib import Path

GEOJSON_PATH = Path(__file__).parent / "map.geojson"


def create_db(geojson_path: Path, db_path: Path) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB instance at db_path and load the GeoJSON outlines."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(f"""
        CREATE TABLE outlines AS
        SELECT * REPLACE (ST_GeomFromWKB(ST_AsWKB(geom)) AS geom)
        FROM ST_Read('{geojson_path}')
    """)
    conn.execute("""
        CREATE TABLE leds (
            id INTEGER PRIMARY KEY,
            geom GEOMETRY
        )
    """)
    return conn


def insert_led(conn: duckdb.DuckDBPyConnection, id: int, lon: float, lat: float) -> None:
    """Insert a 0.75x0.75 degree rectangle centred at (lon, lat) into leds.

    Raises ValueError if it overlaps any existing led.
    """
    half = 0.375
    geom_wkt = (
        f"POLYGON(("
        f"{lon - half} {lat - half}, "
        f"{lon + half} {lat - half}, "
        f"{lon + half} {lat + half}, "
        f"{lon - half} {lat + half}, "
        f"{lon - half} {lat - half}"
        f"))"
    )
    in_outline = conn.execute("""
        SELECT ST_Intersects(geom, ST_GeomFromText($geom)) FROM outlines LIMIT 1
    """, {"geom": geom_wkt}).fetchone()
    if not in_outline or not in_outline[0]:
        raise ValueError(f"LED {id} does not overlap with the outline")

    overlapping_ids = conn.execute("""
        SELECT id FROM leds
        WHERE ST_Intersects(geom, ST_GeomFromText($geom))
          AND NOT ST_IsEmpty(ST_Intersection(geom, ST_GeomFromText($geom)))
    """, {"geom": geom_wkt}).fetchall()
    if overlapping_ids:
        ids = ", ".join(str(r[0]) for r in overlapping_ids)
        raise ValueError(f"LED {id} overlaps existing LED(s): {ids}")
    conn.execute(
        "INSERT INTO leds VALUES ($id, ST_GeomFromText($geom))",
        {"id": id, "geom": geom_wkt},
    )


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.argument("db_path", type=click.Path(path_type=Path))
@click.option(
    "--geojson",
    default=GEOJSON_PATH,
    show_default=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to the outline GeoJSON file.",
)
def init(db_path: Path, geojson: Path) -> None:
    """Create a DuckDB database at DB_PATH loaded with map outlines."""
    create_db(geojson, db_path)
    click.echo(f"Created {db_path} with outlines from {geojson}")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
def coverage(db_path: Path) -> None:
    """Report what percentage of the outline polygon is covered by leds polygons."""
    conn = duckdb.connect(str(db_path))
    conn.execute("LOAD spatial;")
    result = conn.execute("""
        WITH outline AS (
            SELECT geom FROM outlines LIMIT 1
        ),
        leds_union AS (
            SELECT ST_Union_Agg(geom) AS geom FROM leds
        ),
        outside_sum AS (
            SELECT COALESCE(SUM(
                ST_Area(ST_Difference(l.geom, o.geom)) / ST_Area(l.geom) * 100
            ), 0.0) AS total_outside_pct
            FROM leds l, outline o
        )
        SELECT
            CASE
                WHEN lu.geom IS NULL THEN 0.0
                ELSE ST_Area(ST_Intersection(o.geom, lu.geom)) / ST_Area(o.geom) * 100
            END AS coverage_pct,
            os.total_outside_pct
        FROM outline o, leds_union lu, outside_sum os
    """).fetchone()
    click.echo(f"Outline coverage: {result[0]:.2f}%")
    click.echo(f"Sum of LED area outside boundary: {result[1]:.2f}%")


if __name__ == "__main__":
    cli()
