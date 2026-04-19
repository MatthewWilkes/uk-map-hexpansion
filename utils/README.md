# uk-map-hexpansion

Tools for managing LED placements over a UK boundary map, backed by DuckDB with spatial support.

## Setup

```
mise exec -- uv sync
```

## Commands

All commands are run via:

```
mise exec -- uv run python db.py <command> [args]
```

---

### `init`

Create a new DuckDB database, loading the outline polygon from a GeoJSON file.

```
python db.py init <db_path> [--geojson <path>]
```

| Argument / Option | Description |
|---|---|
| `db_path` | Path to the DuckDB file to create |
| `--geojson` | Path to the outline GeoJSON file (default: `map.geojson`) |

**Example:**

```
python db.py init map.db
python db.py init map.db --geojson custom_outline.geojson
```

---

### `add`

Add a new LED as a 0.75×0.75 degree rectangle centred on the given coordinates.

```
python db.py add <db_path> <id> <lon> <lat>
```

| Argument | Description |
|---|---|
| `db_path` | Path to the DuckDB file |
| `id` | Integer ID for the LED |
| `lon` | Longitude of the centre point |
| `lat` | Latitude of the centre point |

Fails if:
- The rectangle does not overlap with the outline at all
- The rectangle overlaps with an existing LED

**Example:**

```
python db.py add map.db 1 -2.5 53.5
```

---

### `list`

List all LEDs in the database, showing their ID and polygon geometry in WKT format.

```
python db.py list <db_path>
```

**Example:**

```
python db.py list map.db
```

---

### `remove`

Remove a LED by its ID.

```
python db.py remove <db_path> <id>
```

| Argument | Description |
|---|---|
| `db_path` | Path to the DuckDB file |
| `id` | ID of the LED to remove |

Fails if no LED with the given ID exists.

**Example:**

```
python db.py remove map.db 1
```

---

### `coverage`

Report coverage statistics for the current LED placements.

```
python db.py coverage <db_path>
```

Outputs two figures:

- **Outline coverage** — percentage of the outline polygon's area covered by all LEDs combined
- **Sum of LED area outside boundary** — sum across all LEDs of the percentage of each LED's area that falls outside the outline

**Example:**

```
python db.py coverage map.db
Outline coverage: 12.34%
Sum of LED area outside boundary: 3.21%
```
