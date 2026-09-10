import itertools
import json
import os
import sqlite3
from collections.abc import KeysView, ValuesView
from datetime import date, datetime, time, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from retriever.utils import flatten


def init():
    global _DATETIME, _PH
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        _PH = "%s"
        _DATETIME = "::timestamptz"
    else:
        _PH = "?"
        _DATETIME = ""


def _cast_value(value):
    if isinstance(value, bool):
        return int(value)
    elif isinstance(value, (list, dict)):
        return json.dumps(value)
    elif isinstance(value, set):
        return json.dumps(sorted(value))
    elif isinstance(value, (datetime, date, time)):
        return value.isoformat()
    else:
        return value


def _build_where_constraint(where_kwargs={}):
    SUPPORTED_OPS = ("=", "!=", "<", ">", "<=", ">=", "in", "like", "between")

    where_kwargs = where_kwargs or {}

    where_parts = []
    where_params = []
    for column, constraints in where_kwargs.items():
        if not isinstance(constraints, (list, tuple, set)):
            constraints = [("=", constraints)]

        for constraint in constraints:
            op, *value = constraint
            value = flatten(value)

            if op.lower() not in SUPPORTED_OPS:
                raise ValueError(f"Invalid operator ({op}) found in constraint: {constraint}")

            if not value or not any(value):
                continue

            if op == "between":
                if len(value) != 2:
                    raise ValueError("Expected exactly two operands for BETWEEN.")

                if not any(value):
                    continue
                elif all(value):
                    right_operand = f"{_PH} AND {_PH}"
                else:
                    right_operand = _PH
                    op = ">=" if value[0] else "<="
                    value = [v for v in value if v]
            elif op == "in":
                right_operand_placeholders = ", ".join([_PH] * len(value))
                right_operand = f"({right_operand_placeholders})"
            else:
                right_operand = _PH

            column_cast = _DATETIME if isinstance(value, (date, time, datetime)) else ""
            where_parts.append(f"{column}{column_cast} {op.upper()} {right_operand}")

            where_params.extend(flatten(value))

    return " AND ".join(where_parts), tuple(where_params)


def _build_conflict_clause(conflict):
    conflict_parts = []
    conflict_params = []
    if conflict:
        if len(conflict) > 1:
            raise ValueError("Cannot interpret conflict with more than one entry.")

        conflict_columns = list(conflict.keys())[0]
        conflict_action = conflict[conflict_columns]

        conflict_columns_str = ", ".join(conflict_columns)
        conflict_parts.append(f"ON CONFLICT ({conflict_columns_str})")
        if conflict_action:
            conflict_update_str = ", ".join(f"{col} = {_PH}" for col in conflict_action)
            conflict_params = list(conflict_action.values())
            conflict_parts.append(f"DO UPDATE SET {conflict_update_str}")
        else:
            conflict_parts.append("DO NOTHING")

    return " ".join(conflict_parts), tuple(conflict_params)


def _build_insert_values(assignments):
    if isinstance(assignments, dict):
        assignments = [assignments]

    raw_columns = {tuple(item.keys()) for item in assignments}
    if len(raw_columns) != 1:
        raise ValueError("Bulk insert is only allowed when all columns are the same.")

    values_pieces = []
    insert_params = []
    for assignment in assignments:
        placeholders_str = ", ".join([f"{_PH}"] * len(assignment))
        values_pieces.append(f"({placeholders_str})")
        insert_params.extend(assignment.values())

    values_clause = f"VALUES {', '.join(values_pieces)}"
    columns_str = ", ".join(list(raw_columns)[0])
    return columns_str, values_clause, tuple(insert_params)


class connection():
    def __init__(self):
        self.db = None

    def __enter__(self):
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            self.db = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        else:
            self.db = sqlite3.connect("showtimes.db")
            self.db.row_factory = sqlite3.Row
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.commit()
        self.db.close()

    def _execute(self, query, params):
        if isinstance(query, list):
            query = " ".join(query)
        
        sql_params = [_cast_value(p) for p in params]

        cur = self.db.cursor()
        cur.execute(query, sql_params)
        return cur

    def select(self, table, columns=None, where=None, *, group_by=None, order_by=None):
        columns = ", ".join(columns or []) or "*"

        query_parts = [f"SELECT {columns}", f"FROM {table}"]

        where_constraint, where_params = _build_where_constraint(where)
        if where_constraint:
            query_parts.append(f"WHERE {where_constraint}")

        if group_by:
            query_parts.append(f"GROUP BY {group_by}")

        if order_by:
            try:
                field, direction = order_by
                direction = "" if direction.lower() not in ("asc", "desc") else direction
            except:
                field, direction = order_by, "asc"
            query_parts.append(f"ORDER BY {field} {direction}")

        cur = self._execute(query_parts, where_params)

        return [dict(row) for row in cur.fetchall()]
    

    def selectone(self, table, columns=None, where=None, *, group_by=None, order_by=None):
        results = self.select(table, columns, where, group_by=group_by, order_by=order_by)
        return results[0] if results else {}


    def update(self, table, assign, where=None):
        if not assign:
            raise ValueError("Request to insert was empty.")

        query_parts = [f"UPDATE {table}"]

        assignment_statements = ", ".join([f"{col} = {_PH}" for col in assign])
        if assignment_statements:
            query_parts.append(f"SET {assignment_statements}")

        where_constraint, where_params = _build_where_constraint(where)
        if where_constraint:
            query_parts.append(f"WHERE {where_constraint}")
        
        self._execute(query_parts, tuple(assign.values()) + where_params)

    def insert(self, table, assignments, *, conflict=None):
        if not assignments:
            raise ValueError("Request to insert was empty.")

        columns_str, values_clause, insert_params = _build_insert_values(assignments)
        query_parts = [f"INSERT INTO {table}({columns_str})", values_clause]

        conflict_clause, conflict_params = _build_conflict_clause(conflict)
        if conflict_clause:
            query_parts.append(conflict_clause)

        self._execute(query_parts, insert_params + conflict_params)


    def delete(self, table, where):
        where_constraint, where_params = _build_where_constraint(where)
        
        query_parts = [f"DELETE FROM {table}", f"WHERE {where_constraint}"]
        self._execute(query_parts, where_params)


init()
