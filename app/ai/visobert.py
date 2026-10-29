"""Model nhà trồng: ViSoBERT fine-tune bằng nhãn bình luận phim (scripts/ml/visobert_finetune.py) - cảm xúc khen/chê/trung lập,
chạy ngay trong tiến trình (CPU trên VPS, MPS/CUDA nếu có), không tốn tiền. Chạy TRƯỚC Bee: câu đủ chắc
(settings.visobert_min_confidence) lấy luôn, chỉ phần còn lại mới gọi model quyết định của Bee (app/ai/decisions.py).

Cần torch + transformers (requirements-ml.txt - VPS cài bản CPU) và thư mục model (settings.visobert_model_dir, nằm ngoài
git). Thiếu thứ nào thì available() = False và bên gọi đi thẳng sang Bee như trước.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_LABELS = {"NEG": "negative", "POS": "positive", "NEU": "neutral"}
_MAX_TOKENS = 256
_ROOT = Path(__file__).resolve().parents[2]

_model: Any = None
_failed = False
_lock = threading.Lock()


def _model_dir() -> Path:
    path = Path(settings.visobert_model_dir)
    return path if path.is_absolute() else _ROOT / path


class _Classifier:
    def __init__(self, model_dir: Path) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, PreTrainedTokenizerFast

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        # AutoTokenizer của transformers 5 lỗi với tokenizer XLM-R của model này - nạp thẳng tokenizer.json.
        self.tokenizer = PreTrainedTokenizerFast.from_pretrained(str(model_dir))
        self.model = AutoModelForSequenceClassification.from_pretrained(str(model_dir)).to(self.device).eval()
        self.id2label = {int(k): _LABELS[v] for k, v in self.model.config.id2label.items()}

    def predict(self, texts: list[str]) -> list[tuple[str, float]]:
        torch = self._torch
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        results: dict[int, tuple[str, float]] = {}
        batch = max(1, settings.visobert_batch_size)
        with torch.inference_mode():
            for start in range(0, len(order), batch):
                idx = order[start : start + batch]
                enc = self.tokenizer(
                    [texts[i] for i in idx], padding=True, truncation=True, max_length=_MAX_TOKENS, return_tensors="pt"
                ).to(self.device)
                probs = torch.softmax(self.model(**enc).logits.float(), dim=-1).cpu()
                conf, label = probs.max(dim=-1)
                for i, c, lab in zip(idx, conf.tolist(), label.tolist(), strict=True):
                    results[i] = (self.id2label[lab], round(c, 4))
        return [results[i] for i in range(len(texts))]


def _get() -> _Classifier | None:
    global _model, _failed
    if _model is not None or _failed or not settings.visobert_enabled:
        return _model
    with _lock:
        if _model is None and not _failed:
            model_dir = _model_dir()
            try:
                if not (model_dir / "model.safetensors").exists():
                    raise FileNotFoundError(str(model_dir))
                _model = _Classifier(model_dir)
                logger.info("visobert_loaded", model_dir=str(model_dir), device=_model.device)
            except Exception as exc:  # noqa: BLE001 - thiếu torch/model: bỏ qua bước này, đi thẳng sang Bee
                _failed = True
                logger.warning("visobert_unavailable", model_dir=str(model_dir), error=str(exc)[:200])
    return _model


async def predict(texts: list[str]) -> list[tuple[str, float]] | None:
    """(nhãn, độ tự tin) cho từng câu, cùng thứ tự; None khi model không dùng được."""
    model = await asyncio.to_thread(_get)
    if model is None or not texts:
        return None
    return await asyncio.to_thread(model.predict, texts)
