"""Phân loại ảnh bìa bài viết: SigLIP base (google/siglip-base-patch16-224, chỉ phần đọc ảnh) lấy vector 768 chiều, rồi một
lớp logistic regression đã train sẵn (cinema_frames_probe.json) chia 4 nhãn:

  screen  chụp màn hình chiếu trong rạp (cảnh phim, credit) - nguồn cho "cảnh được bàn nhiều", cũng là quay lén
  venue   có mặt ở rạp: vé giấy / vé điện tử, ảnh cạnh poster, bắp nước, ghế, sảnh - minh chứng "đã xem"
  event   cinetour, giao lưu đoàn phim, họp báo trong rạp (phần lớn do trang tin đăng)
  other   còn lại (poster, ảnh quảng bá, tin tức, ảnh trong phim, selfie ở nơi khác...)

Probe train ngày 2026-10-20 trên 2.169 ảnh gán tay (bài sau ngày khởi chiếu): đúng 89,4% (cross-validation 5 phần);
"đã xem" (screen + venue) ở ngưỡng 0,7: đúng 89%, bắt được 84%. Bản ghép thêm SigLIP so400m đạt 91% nhưng quá nặng cho
VPS 2 nhân - chưa dùng.

Cần torch + transformers + pillow (đã có ở môi trường chạy ViSoBERT). Không cần sentencepiece: chỉ dùng phần ảnh."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
from PIL import Image

PROBE_PATH = Path(__file__).with_name("cinema_frames_probe.json")
MODEL_TAG = "siglip-base+probe-20261020"
# Ảnh lưu R2 / đưa vào mô hình: cạnh dài tối đa chừng này (mô hình tự thu về 224).
STORE_MAX_SIDE = 384


@dataclass
class FrameLabel:
    label: str
    confidence: float
    probs: dict[str, float]
    embedding: np.ndarray  # float32, đã chuẩn hoá L2 - dùng gom ảnh cùng cảnh


def shrink_jpeg(data: bytes) -> tuple[bytes, Image.Image]:
    """Ảnh gốc -> JPEG cạnh dài STORE_MAX_SIDE (để lưu R2) và ảnh RGB tương ứng (để đưa vào mô hình). Lỗi khi không
    phải ảnh."""
    image = Image.open(io.BytesIO(data)).convert("RGB")
    image.thumbnail((STORE_MAX_SIDE, STORE_MAX_SIDE))
    out = io.BytesIO()
    image.save(out, "JPEG", quality=82)
    return out.getvalue(), image


class CinemaFrameClassifier:
    def __init__(self, batch_size: int = 32) -> None:
        import torch
        from transformers import AutoImageProcessor, SiglipVisionModel

        probe = json.loads(PROBE_PATH.read_text())
        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        if self.device == "cpu":
            # VPS (máy ảo): Conv2d cắt ô ảnh của SigLIP đi qua oneDNN, mà oneDNN đọc kích thước cache CPU của máy ảo ra 0
            # rồi chia cho 0 -> tiến trình chết vì SIGFPE (20/10/2026). Tắt oneDNN: chỉ một lớp conv nhỏ, gần như không chậm.
            torch.backends.mkldnn.enabled = False
        self.batch_size = batch_size
        self.classes: list[str] = probe["classes"]
        self._coef = np.asarray(probe["coef"], dtype=np.float32)
        self._intercept = np.asarray(probe["intercept"], dtype=np.float32)
        self._processor = AutoImageProcessor.from_pretrained(probe["embedding_model"])
        # cast: stub của transformers khiến .to() của model bị suy ra sai kiểu.
        model = cast(torch.nn.Module, SiglipVisionModel.from_pretrained(probe["embedding_model"]))
        self._model = model.to(self.device).eval()

    def embed(self, images: list[Image.Image]) -> np.ndarray:
        torch = self._torch
        chunks = []
        with torch.no_grad():
            for start in range(0, len(images), self.batch_size):
                pixels = self._processor(images=images[start : start + self.batch_size], return_tensors="pt")[
                    "pixel_values"
                ]
                pooled = self._model(pixel_values=pixels.to(self.device)).pooler_output
                chunks.append(torch.nn.functional.normalize(pooled, dim=-1).cpu().numpy())
        return np.concatenate(chunks).astype(np.float32) if chunks else np.zeros((0, self._coef.shape[1]), np.float32)

    def classify(self, images: list[Image.Image]) -> list[FrameLabel]:
        embeddings = self.embed(images)
        logits = embeddings @ self._coef.T + self._intercept
        logits -= logits.max(axis=1, keepdims=True)
        probs = np.exp(logits)
        probs /= probs.sum(axis=1, keepdims=True)
        out = []
        for row, emb in zip(probs, embeddings, strict=True):
            best = int(row.argmax())
            out.append(
                FrameLabel(
                    label=self.classes[best],
                    confidence=float(row[best]),
                    probs={c: round(float(p), 4) for c, p in zip(self.classes, row, strict=True)},
                    embedding=emb,
                )
            )
        return out
