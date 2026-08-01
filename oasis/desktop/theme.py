"""
O.A.S.I.S. Desktop Theme — SYS v2.9 Design Tokens for Flet.

Translates the brand's Obsidian Mode palette, typography, and spacing
into Flet's ft.Theme / ft.ColorScheme so every view renders with the
same premium dark aesthetic the Streamlit consoles use.
"""

import flet as ft


# ── SYS v2.9 Brand Palette (from oasis/ui/theme.py) ────────────────────
TEAL = "#00F5D4"
TEAL_DARK = "#00C4AB"
TEAL_GLOW = "#00F5D466"
DEEP_OBSIDIAN = "#0B1020"
OBSIDIAN_RAISE = "#141A2E"
OBSIDIAN_BORDER = "#1F2740"
PLATFORM_WHITE = "#FFFFFF"
NEUTRAL = "#E0E0E0"
TEXT_PRIMARY = "#FFFFFF"
TEXT_SECONDARY = "#B8C0D9"
TEXT_MUTED = "#7A85A8"
SUCCESS = "#00F5D4"
WARNING = "#FFC857"
DANGER = "#FF4D6D"
INFO = "#7DD3FC"


def build_theme() -> ft.Theme:
    """Construct the O.A.S.I.S. Flet theme matching the SYS v2.9 spec."""
    return ft.Theme(
        color_scheme_seed=TEAL,
        color_scheme=ft.ColorScheme(
            primary=TEAL,
            on_primary=DEEP_OBSIDIAN,
            secondary=TEAL_DARK,
            on_secondary=DEEP_OBSIDIAN,
            surface=OBSIDIAN_RAISE,
            on_surface=TEXT_PRIMARY,
            background=DEEP_OBSIDIAN,
            on_background=TEXT_PRIMARY,
            error=DANGER,
            on_error=PLATFORM_WHITE,
            surface_variant=OBSIDIAN_BORDER,
            on_surface_variant=TEXT_SECONDARY,
            outline=OBSIDIAN_BORDER,
            outline_variant=OBSIDIAN_BORDER,
        ),
        text_theme=ft.TextTheme(
            display_large=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_700),
            display_medium=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_600),
            headline_large=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_600),
            headline_medium=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_500),
            title_large=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_600),
            title_medium=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_500),
            body_large=ft.TextStyle(font_family="Inter"),
            body_medium=ft.TextStyle(font_family="Inter"),
            label_large=ft.TextStyle(font_family="Inter", weight=ft.FontWeight.W_500,
                                     letter_spacing=1.2),
        ),
    )


# ── Reusable Style Helpers ──────────────────────────────────────────────

def card_container(**kwargs) -> ft.Container:
    """A styled card container matching the glassmorphism card aesthetic."""
    defaults = dict(
        bgcolor=OBSIDIAN_RAISE,
        border_radius=10,
        border=ft.border.all(1, OBSIDIAN_BORDER),
        padding=20,
        margin=ft.margin.only(bottom=12),
    )
    defaults.update(kwargs)
    return ft.Container(**defaults)


def spec_tag(text: str, hot: bool = False) -> ft.Container:
    """The brand's signature spec-sheet bracket tag."""
    color = TEAL if hot else TEXT_MUTED
    return ft.Container(
        content=ft.Text(
            f"[ {text} ]",
            size=11,
            font_family="JetBrains Mono",
            style=ft.TextStyle(letter_spacing=2.5),
            weight=ft.FontWeight.W_500,
            color=color,
        ),
        padding=ft.padding.symmetric(vertical=4),
    )


def metric_card(label: str, value: str, sub: str = "",
                status: str = "", help_text: str = "") -> ft.Container:
    """A single KPI metric card."""
    accent_color = {
        "success": SUCCESS, "warning": WARNING,
        "danger": DANGER, "info": INFO,
    }.get(status)

    border = ft.border.all(1, OBSIDIAN_BORDER)
    if accent_color:
        border = ft.border.only(
            left=ft.BorderSide(4, accent_color),
            top=ft.BorderSide(1, OBSIDIAN_BORDER),
            right=ft.BorderSide(1, OBSIDIAN_BORDER),
            bottom=ft.BorderSide(1, OBSIDIAN_BORDER),
        )

    controls = [
        ft.Text(label, size=11, color=TEXT_MUTED,
                font_family="JetBrains Mono",
                style=ft.TextStyle(letter_spacing=1.5),
                weight=ft.FontWeight.W_500),
        ft.Text(value, size=28, weight=ft.FontWeight.W_700,
                color=TEXT_PRIMARY),
    ]
    if sub:
        controls.append(ft.Text(sub, size=12, color=TEXT_SECONDARY))

    return ft.Container(
        content=ft.Column(controls, spacing=4),
        bgcolor=OBSIDIAN_RAISE,
        border_radius=10,
        border=border,
        padding=16,
        tooltip=help_text or None,
        expand=True,
    )


def status_dot(label: str, value: str) -> ft.Row:
    """The brand's teal status ticker dot: ● LABEL: VALUE."""
    return ft.Row(
        controls=[
            ft.Container(
                width=8, height=8,
                border_radius=4,
                bgcolor=TEAL,
                shadow=ft.BoxShadow(
                    spread_radius=2, blur_radius=6,
                    color=TEAL_GLOW,
                ),
            ),
            ft.Text(f"{label}: ", size=11, color=TEXT_MUTED,
                    font_family="JetBrains Mono",
                    style=ft.TextStyle(letter_spacing=1.2),
                    weight=ft.FontWeight.W_500),
            ft.Text(value, size=11, color=TEAL,
                    font_family="JetBrains Mono",
                    style=ft.TextStyle(letter_spacing=1.0),
                    weight=ft.FontWeight.W_700),
        ],
        spacing=6,
    )


def section_header(title: str, icon: str = "") -> ft.Container:
    """Section divider header with optional icon."""
    return ft.Container(
        content=ft.Row([
            ft.Text(f"{icon}  {title}" if icon else title,
                    size=16, weight=ft.FontWeight.W_600,
                    color=TEXT_PRIMARY),
        ]),
        padding=ft.padding.only(top=20, bottom=8),
    )
