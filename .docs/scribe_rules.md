# Scribe Rules - Structural Patterns

This file documents structural mapping patterns for the daily Scribe scan.

## Code Patterns
* Dataflows (`*.Dataflow/mashup.pq`): Mapped to data extraction/load layers.
* Notebooks (`*.Notebook/notebook-content.py`): Mapped to data transformations and analysis layers.
* SQL files (`*.sql`): Mapped to schema changes or analytical queries.
* Tests (`tests/test_*.py`): Used to verify the functionality of code. Ignore internal-only testing helper functions unless they expose new structural fixtures.

## Ignoring internal helpers
* Internal helper functions starting with `_` (e.g., `_create_logger()`) should not be documented in the main structural logs unless they are explicitly exported.
