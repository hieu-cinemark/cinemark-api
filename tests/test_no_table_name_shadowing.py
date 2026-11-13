"""Chặn lỗi "biến/tham số trùng tên bảng" khi dựng query bằng SQLAlchemy.

Ví dụ lỗi: `async def f(posts: list[dict])` rồi bên trong gọi `posts.c.id` - Python sẽ lấy
`list` (không có `.c`) nên ném AttributeError, mà lỗi này thường bị `except` nuốt im lặng
nên query chạy sai mà không ai biết. Cách tránh: import bảng với tên khác
(`from app.repositories.d1.tables import posts as posts_table`).

Test này quét AST mọi file .py trong app/ và scripts/: nếu module import bảng dùng chung với
đúng tên đó (posts/comments/movies/keywords) mà trong một hàm lại có tham số hoặc biến cục bộ
(gán, for, with, comprehension...) che mất tên ấy thì báo lỗi - kể cả khi hàm đó hiện chưa
dùng bảng: dòng code thêm sau sẽ dùng nhầm biến.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = (ROOT / "app", ROOT / "scripts")
TABLES_MODULE = "app.repositories.d1.tables"

# Tên bảng dùng chung - nếu một module khai biến/tham số trùng tên thì có thể đang dùng nhầm.
TABLE_NAMES = ("posts", "comments", "movies", "keywords")


def _imported_table_names(tree: ast.AST) -> set[str]:
    """Tên bảng được import từ app.repositories.d1.tables dưới đúng tên bảng đó."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == TABLES_MODULE:
            for alias in node.names:
                if alias.asname is None and alias.name in TABLE_NAMES:
                    names.add(alias.name)
    return names


def _shadowing_bindings(tree: ast.AST, table_names: set[str]) -> list[tuple[str, int]]:
    """Danh sách (tên, dòng) của tham số hàm / biến cục bộ che mất tên bảng."""
    found: list[tuple[str, int]] = []

    def check_args(args: ast.arguments) -> None:
        for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]:
            if arg.arg in table_names:
                found.append((arg.arg, arg.lineno))
        if args.vararg and args.vararg.arg in table_names:
            found.append((args.vararg.arg, args.vararg.lineno))
        if args.kwarg and args.kwarg.arg in table_names:
            found.append((args.kwarg.arg, args.kwarg.lineno))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            check_args(node.args)
            for inner in ast.walk(node):
                if isinstance(inner, ast.Name) and isinstance(inner.ctx, ast.Store) and inner.id in table_names:
                    found.append((inner.id, inner.lineno))
    return sorted(set(found))


def test_no_local_binding_shadows_shared_table_names() -> None:
    problems: list[str] = []
    for path in sorted(p for d in SCAN_DIRS for p in d.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = _imported_table_names(tree)
        if not imported:
            continue  # module không import bảng dùng chung thì không có rủi ro che tên
        shadowed = _shadowing_bindings(tree, imported)
        if shadowed:
            problems.append(f"{path.relative_to(ROOT)}: {shadowed}")
    assert not problems, (
        "biến/tham số trùng tên bảng - import bảng với tên khác (vd. `posts as posts_table`): " + "; ".join(problems)
    )
