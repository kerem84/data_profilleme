"""Veri kalitesi skoru hesaplama."""

from typing import Any, Dict, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.profiler import ColumnProfile


class QualityScorer:
    """Kolon ve tablo bazinda veri kalitesi skoru hesaplar."""

    def __init__(self, weights: Dict[str, float]):
        self.weights = weights

    def score_column(self, profile: "ColumnProfile") -> Tuple[float, str, List[str]]:
        """
        Kolon kalite skoru hesapla.
        Returns: (score: 0.0-1.0, grade: A-F, flags: list)
        """
        flags: List[str] = []

        # --- Completeness: 1 - null_ratio ---
        completeness = 1.0 - profile.null_ratio

        if profile.null_ratio >= 1.0:
            flags.append("all_null")
        elif profile.null_ratio > 0.5:
            flags.append("high_null")
        elif profile.null_ratio > 0.2:
            flags.append("moderate_null")

        # --- Uniqueness: distinct_count / non_null_count ---
        non_null = profile.distinct_count  # Kullanilabilir distinct
        total_non_null = max(1, int((1 - profile.null_ratio) * max(1, profile.null_count + profile.distinct_count)))

        if profile.distinct_ratio is not None and profile.distinct_ratio > 0:
            uniqueness = min(profile.distinct_ratio, 1.0)
        else:
            uniqueness = 0.0

        if profile.distinct_count == 1 and profile.null_ratio < 1.0:
            flags.append("constant")
            uniqueness = 0.0
        elif profile.distinct_ratio and profile.distinct_ratio >= 0.999:
            flags.append("all_unique")
        elif profile.distinct_count < 10 and profile.data_type not in ("boolean", "bool"):
            flags.append("low_cardinality")

        # --- Consistency: pattern uyumu (string) veya 1.0 (typed) ---
        consistency = 1.0
        if profile.detected_patterns:
            dominant_ratio = max(profile.detected_patterns.values()) if profile.detected_patterns else 0
            consistency = dominant_ratio
            if dominant_ratio < 0.5:
                flags.append("no_dominant_pattern")
        elif profile.data_type in ("text", "character varying", "varchar"):
            # String ama pattern analizi sonucu yok
            consistency = 0.5

        # --- Validity: non-outlier orani (numeric) veya pattern match (string) ---
        validity = 1.0
        if profile.outlier_ratio is not None:
            validity = 1.0 - min(profile.outlier_ratio, 1.0)
            if profile.outlier_ratio > 0.05:
                flags.append("high_outlier")
            elif profile.outlier_ratio > 0.01:
                flags.append("moderate_outlier")
        elif profile.detected_patterns:
            # String validity: en yuksek pattern match orani
            validity = max(profile.detected_patterns.values()) if profile.detected_patterns else 0.5

        # PII suspect
        if profile.detected_patterns:
            pii_patterns = {"email", "phone_tr", "tc_kimlik"}
            for p in pii_patterns:
                if profile.detected_patterns.get(p, 0) > 0.5:
                    flags.append("suspected_pii")
                    break

        # --- Composite score ---
        w = self.weights
        score = (
            completeness * w.get("completeness", 0.35)
            + uniqueness * w.get("uniqueness", 0.20)
            + consistency * w.get("consistency", 0.25)
            + validity * w.get("validity", 0.20)
        )

        score = round(min(max(score, 0.0), 1.0), 4)
        grade = self.grade(score)

        return score, grade, flags

    @staticmethod
    def grade(score: float) -> str:
        """Skoru nota cevir."""
        if score >= 0.9:
            return "A"
        if score >= 0.75:
            return "B"
        if score >= 0.6:
            return "C"
        if score >= 0.4:
            return "D"
        return "F"
