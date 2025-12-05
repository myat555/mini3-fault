"""Dynamic routing that routes around failed nodes."""

from typing import List, Optional
from .config import ProcessSpec
from .health_checker import HealthChecker


class DynamicRouter:
    """Routes queries around failed neighbors."""
    
    def __init__(self, health_checker: HealthChecker):
        self._health_checker = health_checker
    
    def filter_healthy(self, neighbors: List[ProcessSpec]) -> List[ProcessSpec]:
        """Filter neighbors to only include healthy ones."""
        return [n for n in neighbors if self._health_checker.is_healthy(n.id)]
    
    def get_alternative_route(
        self,
        failed_neighbor_id: str,
        all_neighbors: List[ProcessSpec],
    ) -> Optional[List[ProcessSpec]]:
        """Get alternative route when a neighbor fails."""
        healthy = self.filter_healthy(all_neighbors)
        # Exclude the failed neighbor
        alternatives = [n for n in healthy if n.id != failed_neighbor_id]
        return alternatives if alternatives else None

