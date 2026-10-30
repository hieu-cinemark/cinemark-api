"""Model nhà trồng cho ĐỘ LIÊN QUAN của bài viết (2026-10-22): ViSoBERT fine-tune bằng nhãn AI, gồm 21k bài "uncertain" được
Haiku gán lại (scripts/ml/relevance_relabel_haiku.py + scripts/ml/visobert_relevance_finetune.py --two-class --relabel). Đầu vào
là cặp ("tên phim | từ khoá", nội dung bài) giống lúc huấn luyện; ra related / not_related kèm độ tự tin.

Chạy TRƯỚC AI trong classify_post_relevance_kira(): đủ chắc thì lấy luôn (nhanh ~20 ms/bài trên CPU, không tốn lời gọi),
còn lại mới gọi AI. Đo trên 8 phim giữ riêng: khi model nói related thì ~94% khớp AI; not_related ở ngưỡng 0,95 chỉ ~72%
nên ngưỡng not_related đặt cao hơn (người dùng chấp nhận sai số ban đầu để lấy tốc độ). not_related chỉ ẩn bài, không xoá.

Cần torch + transformers (requirements-ml.txt) và thư mục model (settings.visobert_relevance_model_dir, ngoài git). Thiếu
thì available() = False và mọi bài đi thẳng sang AI như trước.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_MAX_TOKENS = 128  # như lúc huấn luyện
_CONTENT_CHARS = 600
_ROOT = Path(__file__).resolve().parents[3]

_model: Any = None
_failed = False
_lock = threading.Lock()


def _model_dir() -> Path:
    path = Path(settings.visobert_relevance_model_dir)
    return path if path.is_absolute() else _ROOT / path


class _Classifier:
    def __init__(self, model_dir: Path) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained(str(model_dir))
        self.model = AutoModelForSequenceClassification.from_pretrained(str(model_dir)).to(self.device).eval()
        self.id2label = {int(k): v for k, v in self.model.config.id2label.items()}
        self._infer_lock = threading.Lock()

    def predict(self, films: list[str], texts: list[str]) -> list[tuple[str, float]]:
        torch = self._torch
        with self._infer_lock, torch.inference_mode():
            enc = self.tokenizer(
                films, texts, padding=True, truncation="only_second", max_length=_MAX_TOKENS, return_tensors="pt"
            ).to(self.device)
            probs = torch.softmax(self.model(**enc).logits.float(), dim=-1).cpu()
            conf, label = probs.max(dim=-1)
        return [(self.id2label[lab], round(c, 4)) for c, lab in zip(conf.tolist(), label.tolist(), strict=True)]


def _get() -> _Classifier | None:
    global _model, _failed
    if _model is not None or _failed or not settings.visobert_relevance_enabled:
        return _model
    with _lock:
        if _model is None and not _failed:
            model_dir = _model_dir()
            try:
                if not (model_dir / "model.safetensors").exists():
                    raise FileNotFoundError(str(model_dir))
                _model = _Classifier(model_dir)
                logger.info("visobert_relevance_loaded", model_dir=str(model_dir), device=_model.device)
            except Exception as exc:  # noqa: BLE001 - thiếu torch/model: mọi bài đi thẳng sang AI
                _failed = True
                logger.warning("visobert_relevance_unavailable", model_dir=str(model_dir), error=str(exc)[:200])
    return _model


def film_text(movie: dict[str, Any], keyword: str | None) -> str:
    return f"{movie.get('title') or ''} | {keyword or ''}".strip(" |")


async def decide(content: str, movie: dict[str, Any], keyword: str | None) -> dict[str, Any] | None:
    """Verdict {"label", "confidence", "reason"} khi model đủ chắc theo ngưỡng của nhãn; None khi chưa chắc / không có model."""
    if _failed or not settings.visobert_relevance_enabled:
        return None  # trả về ngay, không nhường event loop (giữ thứ tự gom lô của bên gọi)
    model = _model if _model is not None else await asyncio.to_thread(_get)
    if model is None:
        return None
    text = " ".join(content[:_CONTENT_CHARS].split())
    label, confidence = (await asyncio.to_thread(model.predict, [film_text(movie, keyword)], [text]))[0]
    threshold = (
        settings.visobert_relevance_related_min if label == "related" else settings.visobert_relevance_not_related_min
    )
    if confidence < threshold:
        return None
    return {"label": label, "confidence": confidence, "reason": f"visobert ({confidence:.2f})"}
