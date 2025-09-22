"""Utilidades para poblar y aplicar filtros de grupo/proyecto en vistas."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Optional, TypeVar

from PySide6.QtWidgets import QComboBox

from ..ui.widgets import SignalBlocker

T = TypeVar("T")


def _normalized(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    return value or None


def current_combo_value(combo: QComboBox) -> Optional[str]:
    """Devuelve el dato asociado al elemento seleccionado."""

    idx = combo.currentIndex()
    return combo.itemData(idx)


def populate_group_filter(
    combo: QComboBox,
    records: Iterable[T],
    *,
    group_getter: Callable[[T], Optional[str]],
) -> None:
    """Rellena el combo de grupos con los valores presentes en los registros."""

    groups = {
        group
        for group in (
            _normalized(group_getter(record))
            for record in records
        )
        if group
    }
    with SignalBlocker(combo):
        combo.clear()
        combo.addItem("Todos", userData=None)
        for group in sorted(groups):
            combo.addItem(group, userData=group)


def update_project_filter(
    group_combo: QComboBox,
    project_combo: QComboBox,
    records: Iterable[T],
    *,
    group_getter: Callable[[T], Optional[str]],
    project_getter: Callable[[T], Optional[str]],
) -> None:
    """Actualiza el combo de proyectos en función del grupo seleccionado."""

    current_group = current_combo_value(group_combo)
    projects = set()
    for record in records:
        group = _normalized(group_getter(record))
        if current_group and group != current_group:
            continue
        project = _normalized(project_getter(record))
        if project:
            projects.add(project)
    with SignalBlocker(project_combo):
        project_combo.clear()
        project_combo.addItem("Todos", userData=None)
        for project in sorted(projects):
            project_combo.addItem(project, userData=project)


def sync_group_project_filters(
    group_combo: QComboBox,
    project_combo: QComboBox,
    records: Iterable[T],
    *,
    group_getter: Callable[[T], Optional[str]],
    project_getter: Callable[[T], Optional[str]],
) -> None:
    """Sincroniza los combos de grupo y proyecto con los registros disponibles."""

    populate_group_filter(group_combo, records, group_getter=group_getter)
    update_project_filter(
        group_combo,
        project_combo,
        records,
        group_getter=group_getter,
        project_getter=project_getter,
    )


def iter_filtered_records(
    records: Iterable[T],
    *,
    group_combo: QComboBox,
    project_combo: QComboBox,
    search_text: Optional[str],
    group_getter: Callable[[T], Optional[str]],
    project_getter: Callable[[T], Optional[str]],
    haystack_builder: Callable[[T], Iterable[str]],
) -> Iterator[T]:
    """Filtra los registros según los combos y el texto de búsqueda."""

    current_group = current_combo_value(group_combo)
    current_project = current_combo_value(project_combo)
    search = (search_text or "").strip().lower()
    for record in records:
        group = _normalized(group_getter(record))
        if current_group and group != current_group:
            continue
        project = _normalized(project_getter(record))
        if current_project and project != current_project:
            continue
        if search:
            haystack = " ".join(
                part.strip()
                for part in haystack_builder(record)
                if part and part.strip()
            ).lower()
            if search not in haystack:
                continue
        yield record
