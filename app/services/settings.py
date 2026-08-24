from __future__ import annotations

from app.db.repository import BaseRepository
from app.models.setting import Setting


class SettingRepository(BaseRepository[Setting]):
    model = Setting
