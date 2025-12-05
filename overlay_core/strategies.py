"""Fairness strategy implementations."""

from abc import ABC, abstractmethod
from typing import Dict, Optional


class FairnessStrategy(ABC):
    """Base class for fairness strategies."""

    @abstractmethod
    def should_admit(self, team: Optional[str], active_per_team: Dict[str, int], max_active: int, per_team_limit: int) -> bool:
        """Determine if request should be admitted."""
        pass


class StrictPerTeamFairness(FairnessStrategy):
    """Strict per-team limits."""

    def should_admit(
        self,
        team: Optional[str],
        active_per_team: Dict[str, int],
        max_active: int,
        per_team_limit: int,
    ) -> bool:
        total_active = sum(active_per_team.values())
        if total_active >= max_active:
            return False
        
        if team:
            team_key = team.lower()
            if active_per_team.get(team_key, 0) >= per_team_limit:
                return False
        
        return True


class WeightedFairness(FairnessStrategy):
    """Weighted fairness based on team load."""

    def should_admit(
        self,
        team: Optional[str],
        active_per_team: Dict[str, int],
        max_active: int,
        per_team_limit: int,
    ) -> bool:
        total_active = sum(active_per_team.values())
        if total_active >= max_active:
            return False
        
        if team:
            team_key = team.lower()
            team_active = active_per_team.get(team_key, 0)
            # Allow slightly over limit if other teams are underutilized
            other_teams_total = sum(v for k, v in active_per_team.items() if k != team_key)
            if team_active >= per_team_limit and other_teams_total > per_team_limit * 0.8:
                return False
        
        return True


class HybridFairness(FairnessStrategy):
    """Hybrid: Strict when high load, weighted when low load."""

    def __init__(self, high_load_threshold: float = 0.8):
        self.high_load_threshold = high_load_threshold
        self._strict = StrictPerTeamFairness()
        self._weighted = WeightedFairness()

    def should_admit(
        self,
        team: Optional[str],
        active_per_team: Dict[str, int],
        max_active: int,
        per_team_limit: int,
    ) -> bool:
        total_active = sum(active_per_team.values())
        load_ratio = total_active / max_active if max_active > 0 else 0
        
        if load_ratio >= self.high_load_threshold:
            return self._strict.should_admit(team, active_per_team, max_active, per_team_limit)
        else:
            return self._weighted.should_admit(team, active_per_team, max_active, per_team_limit)
