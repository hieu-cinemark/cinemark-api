"""Viết query D1 bằng SQLAlchemy Core: SQLAlchemy chỉ dựng câu SQL (dialect SQLite, tham số "?"), việc gửi vẫn qua
d1_query nên giữ nguyên thử lại khi D1 quá tải, chế độ local và chọn database.

Bảng khai ở đây chỉ để dựng query, schema thật do cinemark quản lý (migrations); không gọi metadata.create_all.
Câu cần mẹo riêng của D1 (dấu "+" chặn index, json_each(?1) ghi hàng loạt) vẫn viết SQL thuần với d1_query.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any

from sqlalchemy import MetaData, Select, Table, func, select
from sqlalchemy.dialects import sqlite
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.sql import ClauseElement

from app.clients.d1 import d1_query

metadata = MetaData()
_DIALECT = sqlite.dialect()
D1_MAX_PARAMS = 100  # giới hạn tham số mỗi câu của D1


def compile_sql(stmt: ClauseElement) -> tuple[str, list[Any]]:
    """Câu SQL và danh sách tham số theo thứ tự "?"."""
    # render_postcompile: `cột.in_([...])` bung thành "?, ?, ..." (mặc định SQLAlchemy để placeholder __[POSTCOMPILE_x]).
    compiled = stmt.compile(dialect=_DIALECT, compile_kwargs={"render_postcompile": True})
    names = getattr(compiled, "positiontup", None) or ()  # DDL (CREATE ...) không có tham số
    return str(compiled), [compiled.params[name] for name in names]


async def d1_exec(stmt: ClauseElement, **kwargs: Any) -> list[dict[str, Any]] | None:
    """d1_query cho câu dựng bằng SQLAlchemy; kwargs như d1_query (database, timeout, quiet)."""
    sql, params = compile_sql(stmt)
    return await d1_query(sql, params, **kwargs)


async def d1_insert_many(
    table: Table,
    rows: Sequence[dict[str, Any]],
    *,
    conflict: Iterable[str] | None = None,
    update: Iterable[str] | None = None,
    **kwargs: Any,
) -> bool:
    """INSERT nhiều dòng, tự chia lô cho vừa D1_MAX_PARAMS. conflict: cột khoá của ON CONFLICT; update: cột ghi đè khi
    trùng (None = mọi cột đã truyền trừ khoá, () = bỏ qua dòng trùng). Mọi dòng phải cùng tập cột. False nếu có lô lỗi."""
    if not rows:
        return True
    columns = list(rows[0])
    per_batch = max(1, D1_MAX_PARAMS // len(columns))
    keys = list(conflict or ())
    changed = [c for c in columns if c not in keys] if update is None else list(update)
    ok = True
    for start in range(0, len(rows), per_batch):
        stmt = insert(table).values(list(rows[start : start + per_batch]))
        if keys and changed:
            stmt = stmt.on_conflict_do_update(index_elements=keys, set_={c: stmt.excluded[c] for c in changed})
        elif keys:
            stmt = stmt.on_conflict_do_nothing(index_elements=keys)
        ok = await d1_exec(stmt, **kwargs) is not None and ok
    return ok


def json_values(values: Iterable[Any]) -> Select:
    """Danh sách cho `cột.in_(...)` qua MỘT tham số JSON (json_each) - không vướng giới hạn 100 tham số của D1."""
    each = func.json_each(json.dumps(list(values), ensure_ascii=False)).table_valued("value")
    return select(each.c.value)
