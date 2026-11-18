"""Train mô hình phân loại tầng tác giả (owned / pro / media / outsider) từ data/author_tiers/dataset.jsonl - 18/11/2026.

Dữ liệu do scripts/ml/author_tier_export.py xuất. Đặc trưng ở app/ml/author_tier.py (dùng chung với job gắn nhãn). Chỉ cần numpy /
scipy / scikit-learn - chạy được bằng môi trường ML riêng trên Mac hoặc .venv của cinemark-api trên VPS:

    python scripts/ml/author_tier_train.py                # đo trên tập kiểm tra, không lưu
    python scripts/ml/author_tier_train.py --save         # đo, rồi lưu model nếu không tệ hơn model đang dùng
    python scripts/ml/author_tier_train.py --save --force # lưu dù tệ hơn

Mô hình: hồi quy logistic, class_weight="balanced" (outsider chiếm ~90%). Tách tập kiểm tra theo handle (StratifiedGroupKFold).
Điểm so sánh giữa hai model: macro-F1 trên tập kiểm tra - trung bình F1 của 4 tầng, nên tầng thiểu số (pro / media / owned) nặng
ký như outsider. Model mới chỉ thay model cũ khi macro-F1 không thấp hơn quá MAX_REGRESSION.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, f1_score
from sklearn.model_selection import StratifiedGroupKFold

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))  # chạy bằng đường dẫn file vẫn import được app.ml

from app.ml.author_tier import PLATFORMS, TIERS, Featurizer

DATA = ROOT / "data" / "author_tiers" / "dataset.jsonl"
MODEL_DIR = ROOT / "models" / "author_tier"
SEED = 7
MAX_REGRESSION = 0.01


def load(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def new_model(c: float) -> LogisticRegression:
    return LogisticRegression(max_iter=1000, C=c, class_weight="balanced", solver="lbfgs")


def coverage_table(y_true: list[str], proba: np.ndarray, classes: list[str]) -> list[dict]:
    pred = np.asarray(classes)[proba.argmax(axis=1)]
    top = proba.max(axis=1)
    truth = np.asarray(y_true)
    rows = []
    print("\nNgưỡng tự tin | % tác giả tự gắn | khớp nhãn (tổng) | khớp khi gắn tầng khác outsider (số ca)")
    for threshold in (0.5, 0.6, 0.75, 0.85, 0.9, 0.95):
        mask = top >= threshold
        if not mask.any():
            continue
        minority = mask & (pred != "outsider")
        row = {
            "threshold": threshold,
            "coverage": float(mask.mean()),
            "agreement": float((pred[mask] == truth[mask]).mean()),
            "minority_agreement": float((pred[minority] == truth[minority]).mean()) if minority.any() else None,
            "minority_cases": int(minority.sum()),
        }
        rows.append(row)
        print(
            f"  >= {threshold:.2f}      | {row['coverage']:6.1%}          | {row['agreement']:6.1%}           | "
            f"{(row['minority_agreement'] or 0):6.1%} ({row['minority_cases']:,})"
        )
    return rows


def evaluate(rows: list[dict], c: float) -> dict:
    labels = [r["tier"] for r in rows]
    splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=SEED)
    train_idx, test_idx = next(splitter.split(rows, labels, [r["handle"] for r in rows]))
    train, test = [rows[i] for i in train_idx], [rows[i] for i in test_idx]
    print(f"train {len(train):,} / test {len(test):,} (tách theo handle)", flush=True)
    started = time.monotonic()
    featurizer = Featurizer().fit(train)
    x_train, x_test = featurizer.transform(train), featurizer.transform(test)
    print(f"đặc trưng: {x_train.shape[1]:,} cột; đang train...", flush=True)
    model = new_model(c).fit(x_train, [r["tier"] for r in train])
    print(f"train xong ({time.monotonic() - started:.0f}s)", flush=True)
    y_test = [r["tier"] for r in test]
    proba = model.predict_proba(x_test)
    classes = list(model.classes_)
    pred = np.asarray(classes)[proba.argmax(axis=1)]
    print("\n=== Kết quả trên tập kiểm tra ===")
    print(classification_report(y_test, pred, labels=list(TIERS), digits=3, zero_division=0))
    print("Ma trận nhầm (hàng = nhãn thật, cột = dự đoán):", list(TIERS))
    print(confusion_matrix(y_test, pred, labels=list(TIERS)))
    for platform in PLATFORMS:
        idx = [i for i, r in enumerate(test) if r["platform"] == platform]
        if len(idx) >= 50:
            sub_true, sub_pred = [y_test[i] for i in idx], [pred[i] for i in idx]
            macro = f1_score(sub_true, sub_pred, labels=list(TIERS), average="macro", zero_division=0)
            print(f"  {platform:9} {len(idx):6,} tác giả  macro-F1 {macro:.3f}")
    report = classification_report(y_test, pred, labels=list(TIERS), output_dict=True, zero_division=0)
    return {
        "macro_f1": float(f1_score(y_test, pred, labels=list(TIERS), average="macro", zero_division=0)),
        "per_tier": {tier: {k: float(report[tier][k]) for k in ("precision", "recall", "f1-score")} for tier in TIERS},
        "coverage": coverage_table(y_test, proba, classes),
        "test_size": len(test),
    }


def current_metrics() -> dict | None:
    path = MODEL_DIR / "metrics.json"
    return json.loads(path.read_text()) if path.exists() else None


def main(args: argparse.Namespace) -> int:
    rows = load(DATA)
    counts = Counter(r["tier"] for r in rows)
    print(f"{len(rows):,} tác giả có nhãn: {dict(counts)}", flush=True)
    metrics = evaluate(rows, args.c)
    print(f"\nmacro-F1: {metrics['macro_f1']:.3f}")
    if not args.save:
        return 0
    previous = current_metrics()
    if previous and not args.force and metrics["macro_f1"] < previous["macro_f1"] - MAX_REGRESSION:
        print(f"KHÔNG thay model: macro-F1 {metrics['macro_f1']:.3f} < model đang dùng {previous['macro_f1']:.3f}")
        return 2
    import joblib
    import sklearn

    featurizer = Featurizer().fit(rows)
    final = new_model(args.c).fit(featurizer.transform(rows), [r["tier"] for r in rows])
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    tmp = MODEL_DIR / "model.joblib.tmp"
    joblib.dump({"featurizer": featurizer, "model": final, "tiers": list(TIERS)}, tmp)
    tmp.replace(MODEL_DIR / "model.joblib")  # thay nguyên khối: job đang đọc không bao giờ thấy file dở
    (MODEL_DIR / "metrics.json").write_text(
        json.dumps(
            {
                **metrics,
                "trained_at": datetime.now(tz=UTC).isoformat(),
                "labels": dict(counts),
                "sklearn": sklearn.__version__,
                "c": args.c,
            },
            ensure_ascii=False,
            indent=1,
        )
    )
    print(f"đã lưu model (train trên toàn bộ {len(rows):,} tác giả) -> {MODEL_DIR}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--c", type=float, default=4.0, help="độ mạnh regularization nghịch đảo của hồi quy logistic")
    parser.add_argument(
        "--save", action="store_true", help="train lại trên toàn bộ dữ liệu và lưu model (nếu không tệ hơn)"
    )
    parser.add_argument("--force", action="store_true", help="lưu dù macro-F1 thấp hơn model đang dùng")
    sys.exit(main(parser.parse_args()))
