# news_crawler/models.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class Article:
    id: str
    title: str
    link: str
    published: Optional[str] = None
    source: Optional[str] = None
    content: Optional[str] = None
    matched_tags: List[str] = field(default_factory=list)
    ts: Optional[int] = None
    label: Optional[str] = None
    label_score: Optional[float] = None
    cluster_id: Optional[int] = None

    def published_dt(self) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(self.published) if self.published else None
        except Exception:
            return None
