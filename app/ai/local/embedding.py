"""Embedding câu cho gom chủ đề (Social Topic bước 4): intfloat/multilingual-e5-small, mean-pooling + chuẩn hoá,
nên tích vô hướng = cosine. Đo 26/10/2026: ~206 câu/giây trên 2 luồng CPU (cỡ VPS), ~470 MB tải về lần đầu
(cache ở ~/.cache/huggingface). Model nạp lười, một lần mỗi tiến trình."""

from __future__ import annotations

import os
from functools import lru_cache

import numpy as np

MODEL_ID = "intfloat/multilingual-e5-small"
PREFIX = "query: "  # e5 yêu cầu tiền tố; "query:" cho câu ngắn kiểu comment
MAX_LEN = 128
DIM = 384

@lru_cache(maxsize=1)
def _model():
    import torch
    from transformers import AutoModel, AutoTokenizer
    
    torch.set_num_threads(int(os.getenv("EMBED_THREADS", "2")))
    device = "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModel.from_pretrained(MODEL_ID).to(device).eval()
    
    return tokenizer, model, device

def embed_texts(texts: list[str], batch: int = 128) -> np.ndarray:
    """(n, 384) float32, mỗi dòng có độ dài 1."""
    if not texts:
        return np.zeros((0, DIM), dtype=np.float32)
    import torch
    
    tokenizer, model, device = _model()
    out = []
    with torch.inference_mode():
        for start in range(0, len(texts), batch):
            enc = tokenizer(
                [PREFIX + t for t in texts[start : start + batch]],
                padding=True, truncation=True, max_length=MAX_LEN, return_tensors="pt"
            ).to(device)
            hidden = model(**enc).last_hidden_state
            mask = enc["attention_mask"].unsqueeze(-1).to(hidden.dtype)
            pooled = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
            out.append(torch.nn.functional.normalize(pooled, dim=-1).float().cpu().numpy())
            
    return np.concatenate(out)
    