"""Chart color roles, values taken from the dataviz skill's validated reference
palette (categorical order and sequential ramp both pass its CVD/contrast
checks as-is -- see the skill's references/palette.md for the validation
detail). Charts reference these by role, not raw hex.
"""

from __future__ import annotations

# Fixed order -- never cycled/reassigned by filter or rank. 1-3 validate
# all-pairs and are all this dashboard ever needs (DAU/WAU/MAU is 3 series).
CATEGORICAL = [
    "#2a78d6",  # slot 1 -- blue
    "#eb6834",  # slot 2 -- orange
    "#1baf7a",  # slot 3 -- aqua
]

# Sequential blue ramp, light -> dark, for continuous magnitude (the
# retention heatmap's retention_rate).
SEQUENTIAL_BLUE = [
    "#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec",
    "#5598e7", "#3987e5", "#2a78d6", "#256abf", "#1c5cab",
    "#184f95", "#104281", "#0d366b",
]

# Ordinal ramp for the funnel's ordered stages -- must not start lighter than
# step 250 (#86b6ef) so the first stage still clears 2:1 contrast.
ORDINAL_BLUE = ["#86b6ef", "#5598e7", "#2a78d6", "#184f95"]

GRIDLINE = "#e1e0d9"
MUTED_INK = "#898781"
