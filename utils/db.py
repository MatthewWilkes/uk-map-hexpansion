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
    inside_pct = conn.execute("""
        SELECT ST_Area(ST_Intersection(geom, ST_GeomFromText($geom))) / ST_Area(ST_GeomFromText($geom))
        FROM outlines LIMIT 1
    """, {"geom": geom_wkt}).fetchone()
    if not inside_pct or inside_pct[0] < 0.50:
        pct = (inside_pct[0] * 100) if inside_pct else 0.0
        raise ValueError(f"LED {id} is only {pct:.1f}% inside the outline (minimum 50%)")

    overlapping_ids = conn.execute("""
        SELECT id FROM leds
        WHERE ST_Area(ST_Intersection(geom, ST_GeomFromText($geom))) > 0
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


@cli.command(name="list")
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
def list_leds(db_path: Path) -> None:
    """List all LEDs in the database."""
    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute("LOAD spatial;")
    rows = conn.execute("SELECT id, ST_AsText(geom) FROM leds ORDER BY id").fetchall()
    if not rows:
        click.echo("No LEDs.")
        return
    for led_id, geom in rows:
        click.echo(f"{led_id}\t{geom}")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
@click.argument("id", type=int)
@click.argument("lon", type=float)
@click.argument("lat", type=float)
def add(db_path: Path, id: int, lon: float, lat: float) -> None:
    """Add a LED at centre LON LAT."""
    conn = duckdb.connect(str(db_path))
    conn.execute("LOAD spatial;")
    insert_led(conn, id, lon, lat)
    click.echo(f"Added LED {id} at ({lon}, {lat})")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
@click.argument("id", type=int)
def remove(db_path: Path, id: int) -> None:
    """Remove LED by ID."""
    conn = duckdb.connect(str(db_path))
    conn.execute("DELETE FROM leds WHERE id = $id", {"id": id})
    if conn.execute("SELECT changes()").fetchone()[0] == 0:
        raise click.ClickException(f"No LED with id {id}")
    click.echo(f"Removed LED {id}")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
@click.option("--start-id", default=1, show_default=True, help="Starting ID for inserted LEDs.")
def optimize(db_path: Path, start_id: int) -> None:
    """Place LEDs to maximise outline coverage while minimising area outside the boundary.

    Tries a grid of phase offsets and picks the arrangement with the highest
    ratio of inside-boundary area to total LED area, then inserts all LEDs.
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("LOAD spatial;")

    step = 0.75
    # UK bounding box with margin
    lon_min, lon_max = -7.5, 2.5
    lat_min, lat_max = 49.5, 59.5
    # Quarter-step offsets give 16 distinct phase combinations
    phase_steps = [i * step / 4 for i in range(4)]

    best_score = -1.0
    best_candidates: list[tuple[float, float]] = []

    for lon_off in phase_steps:
        for lat_off in phase_steps:
            lons = [round(lon_min + lon_off + i * step, 6)
                    for i in range(int((lon_max - lon_min) / step) + 2)
                    if lon_min + lon_off + i * step < lon_max]
            lats = [round(lat_min + lat_off + i * step, 6)
                    for i in range(int((lat_max - lat_min) / step) + 2)
                    if lat_min + lat_off + i * step < lat_max]

            centres = [(lon, lat) for lon in lons for lat in lats]
            half = step / 2

            # Build a VALUES table of all candidates and score them in one query
            values = ", ".join(
                f"({lon}, {lat}, ST_GeomFromText("
                f"'POLYGON(({lon-half} {lat-half},{lon+half} {lat-half},"
                f"{lon+half} {lat+half},{lon-half} {lat+half},{lon-half} {lat-half}))'))"
                for lon, lat in centres
            )
            row = conn.execute(f"""
                WITH candidates(lon, lat, geom) AS (VALUES {values}),
                outline AS (SELECT geom FROM outlines LIMIT 1)
                SELECT
                    SUM(ST_Area(ST_Intersection(o.geom, c.geom))) AS inside,
                    SUM(ST_Area(c.geom))                          AS total
                FROM candidates c, outline o
                WHERE ST_Intersects(o.geom, c.geom)
            """).fetchone()

            inside, total = row if row else (0.0, 1.0)
            score = (inside or 0.0) / (total or 1.0)

            if score > best_score:
                best_score = score
                # Re-fetch only the overlapping centres for this offset
                best_candidates = [
                    (float(lon), float(lat))
                    for lon, lat in conn.execute(f"""
                        WITH candidates(lon, lat, geom) AS (VALUES {values}),
                        outline AS (SELECT geom FROM outlines LIMIT 1)
                        SELECT lon, lat FROM candidates c, outline o
                        WHERE ST_Intersects(o.geom, c.geom)
                        ORDER BY lon, lat
                    """).fetchall()
                ]

    click.echo(
        f"Best grid covers {best_score * 100:.2f}% of total LED area inside boundary "
        f"({len(best_candidates)} LEDs)"
    )

    inserted = 0
    led_id = start_id
    for lon, lat in best_candidates:
        try:
            insert_led(conn, led_id, lon, lat)
            inserted += 1
        except ValueError:
            pass
        led_id += 1

    click.echo(f"Inserted {inserted} of {len(best_candidates)} LEDs starting at id {start_id}")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
@click.option("--start-id", default=1, show_default=True, help="Starting ID for inserted LEDs.")
def fill(db_path: Path, start_id: int) -> None:
    """Place LEDs edge-first, then fill the interior.

    Pass 1 — edge: centres LEDs on each vertex of the outline polygon so tiles
    straddle the coastline as closely as possible. Tiles that don't meet the
    75% inside threshold are skipped.

    Pass 2 — interior: covers the remaining interior with a regular grid,
    skipping any position that overlaps an already-placed tile.
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("LOAD spatial;")

    step = 0.75
    inserted = 0
    led_id = start_id

    def _try_insert(lon: float, lat: float) -> bool:
        nonlocal inserted, led_id
        try:
            insert_led(conn, led_id, lon, lat)
            inserted += 1
            return True
        except ValueError:
            return False
        finally:
            led_id += 1

    # Pass 1: place LEDs centred on each boundary vertex
    ring_wkt = conn.execute(
        "SELECT ST_AsText(ST_ExteriorRing(geom)) FROM outlines LIMIT 1"
    ).fetchone()[0]
    coords_str = ring_wkt[ring_wkt.index("(") + 1:ring_wkt.rindex(")")]
    edge_centres = [
        (float(p.split()[0]), float(p.split()[1]))
        for p in coords_str.split(",")
    ]
    for lon, lat in edge_centres:
        _try_insert(lon, lat)
    click.echo(f"Edge pass: {inserted} LEDs placed from {len(edge_centres)} boundary vertices")

    edge_inserted = inserted

    # Pass 2: regular grid fill for the interior
    lon_min, lon_max = -7.5, 2.5
    lat_min, lat_max = 49.5, 59.5
    grid = [
        (round(lon_min + i * step, 6), round(lat_min + j * step, 6))
        for i in range(int((lon_max - lon_min) / step) + 2)
        for j in range(int((lat_max - lat_min) / step) + 2)
        if lon_min + i * step < lon_max and lat_min + j * step < lat_max
    ]
    # Only consider positions whose centre is strictly inside the outline
    values = ", ".join(f"({lon}, {lat})" for lon, lat in grid)
    interior = [
        (float(lon), float(lat))
        for lon, lat in conn.execute(f"""
            WITH pts(lon, lat) AS (VALUES {values}),
            outline AS (SELECT geom FROM outlines LIMIT 1)
            SELECT lon, lat FROM pts, outline
            WHERE ST_Contains(geom, ST_Point(lon, lat))
            ORDER BY lon, lat
        """).fetchall()
    ]
    for lon, lat in interior:
        _try_insert(lon, lat)

    interior_inserted = inserted - edge_inserted
    click.echo(f"Interior pass: {interior_inserted} LEDs placed from {len(interior)} candidates")
    click.echo(f"Total: {inserted} LEDs inserted starting at id {start_id}")


@cli.command()
@click.argument("db_path", type=click.Path(exists=True, path_type=Path))
@click.argument("output", type=click.Path(path_type=Path))
def export(db_path: Path, output: Path) -> None:
    """Export all LEDs as a GeoJSON FeatureCollection to OUTPUT."""
    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute("LOAD spatial;")
    conn.execute(f"""
        COPY (
            SELECT ST_GeomFromWKB(ST_AsWKB(geom)) AS geom, id AS id
            FROM leds
            ORDER BY id
        ) TO '{output}' WITH (FORMAT GDAL, DRIVER 'GeoJSON')
    """)
    count = conn.execute("SELECT COUNT(*) FROM leds").fetchone()[0]
    click.echo(f"Exported {count} LEDs to {output}")


if __name__ == "__main__":
    cli()
