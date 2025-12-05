import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .config import ProcessSpec


TEAM_DATE_BOUNDS = {
    "green": ("20200810", "20200820"),
    "pink": ("20200821", "20200924"),
}

ROLE_WEIGHTS = {
    "leader": 1,
    "team_leader": 2,
    "worker": 3,
    "__default__": 1,
}

ROLE_PRIORITY = {
    "leader": 0,
    "team_leader": 1,
    "worker": 2,
}


class DataStore:
    """Local dataset accessor responsible for enforcing team-specific slices."""

    def __init__(
        self,
        process_id: str,
        team: str,
        dataset_root: str = "datasets/2020-fire/data",
        date_bounds: Optional[Tuple[str, str]] = None,
        team_members: Optional[List["ProcessSpec"]] = None,
        role_weights: Optional[Dict[str, int]] = None,
    ):
        self.process_id = process_id
        self.team = team.lower()
        self.dataset_root = Path(dataset_root)
        self._records: List[Dict[str, object]] = []
        self._files_loaded = 0
        self._team_members = team_members or []
        self._role_weights = role_weights or ROLE_WEIGHTS
        self._explicit_bounds = date_bounds
        self._team_bounds = self._resolve_team_bounds()
        self._available_dates = self._list_available_dates()
        self._selected_dates = self._determine_selected_dates()
        self._selected_date_set = set(self._selected_dates)
        self._load()

    @property
    def records_loaded(self) -> int:
        return len(self._records)

    @property
    def files_loaded(self) -> int:
        return self._files_loaded

    def _load(self) -> None:
        if not self.dataset_root.exists():
            raise FileNotFoundError(f"Dataset root missing: {self.dataset_root}")

        for date_dir in sorted(self.dataset_root.iterdir()):
            if not date_dir.is_dir():
                continue
            date_str = date_dir.name
            if date_str not in self._selected_date_set:
                continue

            for csv_file in sorted(date_dir.glob("*.csv")):
                self._load_file(csv_file, date_str)

        print(f"[DataStore] {self.process_id} loaded {self.records_loaded} rows from {self.files_loaded} files.", flush=True)

    def _load_file(self, path: Path, date_str: str) -> None:
        try:
            with path.open("r", encoding="utf-8") as handle:
                reader = csv.reader(handle)
                for row in reader:
                    if not row or row[0].strip('"').lower() == "latitude":
                        continue
                    record = self._convert_row(row, date_str)
                    if record:
                        self._records.append(record)
            self._files_loaded += 1
        except Exception as exc:
            print(f"[DataStore] failed to load {path}: {exc}", flush=True)

    @staticmethod
    def _convert_row(row: List[str], date_str: str) -> Optional[Dict[str, object]]:
        try:
            return {
                "latitude": float(row[0].strip('"')),
                "longitude": float(row[1].strip('"')),
                "timestamp": row[2].strip('"'),
                "parameter": row[3].strip('"'),
                "value": float(row[4].strip('"')) if row[4].strip('"') else 0.0,
                "unit": row[5].strip('"'),
                "raw_concentration": float(row[6].strip('"')) if len(row) > 6 and row[6].strip('"') else 0.0,
                "aqi": int(row[7].strip('"')) if len(row) > 7 and row[7].strip('"') else 0,
                "category": int(row[8].strip('"')) if len(row) > 8 and row[8].strip('"') else 0,
                "site_name": row[9].strip('"') if len(row) > 9 else "",
                "agency_name": row[10].strip('"') if len(row) > 10 else "",
                "aqs_id": row[11].strip('"') if len(row) > 11 else "",
                "full_aqs_id": row[12].strip('"') if len(row) > 12 else "",
                "date": date_str,
            }
        except (ValueError, IndexError):
            return None

    def query(self, filters: Dict[str, object], limit: Optional[int] = None) -> List[Dict[str, object]]:
        """Return dataset rows that match filters up to limit."""
        remaining = limit or filters.get("limit")
        remaining = int(remaining) if remaining else len(self._records)
        remaining = max(1, remaining)

        results: List[Dict[str, object]] = []
        for record in self._records:
            if self._matches(record, filters):
                results.append(record)
                if len(results) >= remaining:
                    break
        return results

    @staticmethod
    def _matches(record: Dict[str, object], filters: Dict[str, object]) -> bool:
        parameter = filters.get("parameter")
        if parameter and str(record["parameter"]).lower() != str(parameter).lower():
            return False

        min_val = filters.get("min_value")
        if min_val is not None and record["value"] < float(min_val):
            return False

        max_val = filters.get("max_value")
        if max_val is not None and record["value"] > float(max_val):
            return False

        date_start = filters.get("date_start")
        if date_start and str(record["date"]) < str(date_start):
            return False

        date_end = filters.get("date_end")
        if date_end and str(record["date"]) > str(date_end):
            return False

        lat_min = filters.get("lat_min")
        if lat_min is not None and record["latitude"] < float(lat_min):
            return False

        lat_max = filters.get("lat_max")
        if lat_max is not None and record["latitude"] > float(lat_max):
            return False

        lon_min = filters.get("lon_min")
        if lon_min is not None and record["longitude"] < float(lon_min):
            return False

        lon_max = filters.get("lon_max")
        if lon_max is not None and record["longitude"] > float(lon_max):
            return False

        return True

    def stats(self) -> Dict[str, int]:
        return {
            "records": self.records_loaded,
            "files": self.files_loaded,
        }

    def _resolve_team_bounds(self) -> Tuple[str, str]:
        """Return the overall bounds for the team."""
        if self._explicit_bounds:
            return self._explicit_bounds
        bounds = TEAM_DATE_BOUNDS.get(self.team)
        if not bounds:
            raise ValueError(f"No date bounds for team '{self.team}'.")
        return bounds

    def _list_available_dates(self) -> List[str]:
        """Collect all date directories that fall within the team bounds."""
        lower, upper = self._team_bounds
        dates: List[str] = []
        if not self.dataset_root.exists():
            raise FileNotFoundError(f"Dataset root missing: {self.dataset_root}")

        for date_dir in sorted(self.dataset_root.iterdir()):
            if not date_dir.is_dir():
                continue
            date_str = date_dir.name
            if lower <= date_str <= upper:
                dates.append(date_str)

        if not dates:
            raise ValueError(f"No dataset directories available for team '{self.team}'.")
        return dates

    def _determine_selected_dates(self) -> List[str]:
        """Determine which dates this process is responsible for loading."""
        if self._explicit_bounds:
            lower, upper = self._explicit_bounds
            subset = [date for date in self._available_dates if lower <= date <= upper]
            return subset or self._available_dates

        if not self._team_members:
            return self._available_dates

        shares = self._compute_member_shares(self._available_dates)
        selection = shares.get(self.process_id)
        if not selection:
            # Fallback to entire range to avoid empty datasets
            return self._available_dates
        return selection

    def _compute_member_shares(self, dates: List[str]) -> Dict[str, List[str]]:
        """Split available dates among team members using weighted slices."""
        same_team = [member for member in self._team_members if member.team.lower() == self.team]
        if not same_team:
            return {self.process_id: dates}

        ordered_members = sorted(
            same_team,
            key=lambda m: (ROLE_PRIORITY.get(m.role.lower(), 99), m.id),
        )

        weights: List[int] = [
            self._role_weights.get(member.role.lower(), self._role_weights.get("__default__", 1))
            for member in ordered_members
        ]
        total_weight = sum(weights) or len(ordered_members)
        total_days = len(dates)

        base_counts: List[int] = []
        remainders: List[float] = []
        for weight in weights:
            exact = (total_days * weight) / total_weight
            count = int(exact)
            base_counts.append(count)
            remainders.append(exact - count)

        assigned = sum(base_counts)
        remaining = total_days - assigned

        # Ensure members with zero allocation get at least one day when possible
        for idx, count in enumerate(base_counts):
            if remaining <= 0:
                break
            if count == 0:
                base_counts[idx] = 1
                remaining -= 1

        # Distribute any remaining days based on remainders and weight priority
        while remaining > 0:
            idx = max(
                range(len(ordered_members)),
                key=lambda i: (
                    remainders[i],
                    weights[i],
                    -ROLE_PRIORITY.get(ordered_members[i].role.lower(), 99),
                    ordered_members[i].id,
                ),
            )
            base_counts[idx] += 1
            remaining -= 1

        allocations: Dict[str, List[str]] = {}
        cursor = 0
        for member, count in zip(ordered_members, base_counts):
            slice_dates = dates[cursor : cursor + count]
            cursor += count
            allocations[member.id] = slice_dates
        return allocations

