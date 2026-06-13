"""
OASIS shared UI library (U1 + U0).

The single source of truth for client-facing look & feel, replacing the
per-app inline glassmorphism CSS. Adopts the SYS v2.9 Visual System Guide
(teal-turquoise / deep slate / platform white; Montserrat + Space Mono).

Public surface:
    from oasis.ui import theme, components
    theme.inject_theme()                  # once per page, in the shell
    components.metric_card("Cash Used", "KES 1.2M")
"""

from . import theme, components  # noqa: F401
