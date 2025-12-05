import threading
import time
from collections import defaultdict
from typing import Dict, Optional

from .strategies import FairnessStrategy, StrictPerTeamFairness


class RequestAdmissionController:
    """Controls concurrent queries for fairness between teams."""

    def __init__(
        self,
        max_active: int = 100,
        per_team_limit: int = 60,
        fairness_strategy: Optional[FairnessStrategy] = None,
    ):
        self.max_active = max_active
        self.per_team_limit = per_team_limit
        self._fairness = fairness_strategy or StrictPerTeamFairness()
        self._lock = threading.Lock()
        self._active: Dict[str, Dict[str, object]] = {}
        self._per_team = defaultdict(int)
        self._rejections = 0

    def admit(self, uid: str, team: Optional[str]) -> bool:
        team_key = (team or "").lower()
        with self._lock:
            active_per_team = dict(self._per_team)
            if not self._fairness.should_admit(
                team_key, active_per_team, self.max_active, self.per_team_limit
            ):
                self._rejections += 1
                return False

            self._active[uid] = {"team": team_key, "start_ts": time.time()}
            if team_key:
                self._per_team[team_key] += 1
            return True

    def release(self, uid: str) -> None:
        with self._lock:
            info = self._active.pop(uid, None)
            if info and info["team"]:
                self._per_team[info["team"]] = max(0, self._per_team[info["team"]] - 1)

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    @property
    def rejection_count(self) -> int:
        with self._lock:
            return self._rejections

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            return {
                "active": len(self._active),
                "per_team": dict(self._per_team),
                "rejections": self._rejections,
            }

