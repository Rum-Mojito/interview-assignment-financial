from __future__ import annotations

from collections import deque

from src.schema.models import SchemaDefinition


def plan_table_order(schema: SchemaDefinition) -> list[str]:
    """Build DAG topological order so parent tables are generated first."""

    table_names = [table.name for table in schema.tables]
    dependencies: dict[str, set[str]] = {table_name: set() for table_name in table_names}
    downstream: dict[str, set[str]] = {table_name: set() for table_name in table_names}

    for table in schema.tables:
        for foreign_key_column in table.foreign_key_columns():
            foreign_key = foreign_key_column.foreign_key
            if foreign_key is None:
                continue
            parent_table = foreign_key.referenced_table
            dependencies[table.name].add(parent_table)
            downstream[parent_table].add(table.name)

    queue: deque[str] = deque(
        sorted([table_name for table_name, requires in dependencies.items() if not requires])
    )
    ordered_tables: list[str] = []

    while queue:
        current_table = queue.popleft()
        ordered_tables.append(current_table)
        for child_table in sorted(downstream[current_table]):
            dependencies[child_table].remove(current_table)
            if not dependencies[child_table]:
                queue.append(child_table)

    if len(ordered_tables) != len(table_names):
        raise ValueError("cyclic foreign key dependencies detected in schema")
    return ordered_tables
