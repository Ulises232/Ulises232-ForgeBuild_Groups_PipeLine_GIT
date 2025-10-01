"""Consultas y helpers para el catálogo de empresas."""
from __future__ import annotations

from typing import Iterable, List, Optional

from .branch_store import (
    Company,
    IncidenceType,
    delete_company as _delete_company,
    delete_incidence_type as _delete_incidence_type,
    list_companies as _list_companies,
    list_incidence_types as _list_incidence_types,
    upsert_company as _upsert_company,
    upsert_incidence_type as _upsert_incidence_type,
)


def list_companies(*, group: Optional[str] = None) -> List[Company]:
    """Regresa las empresas registradas, opcionalmente filtradas por grupo."""

    companies = _list_companies()
    if group is None:
        return companies
    normalized = (group or "").strip()
    if not normalized:
        return companies
    return [company for company in companies if (company.group_name or "") == normalized]


def find_company(company_id: Optional[int]) -> Optional[Company]:
    """Busca una empresa por su identificador."""

    if company_id is None:
        return None
    for company in _list_companies():
        if company.id == company_id:
            return company
    return None


def save_company(company: Company) -> Company:
    """Inserta o actualiza una empresa en el catálogo."""

    return _upsert_company(company)


def remove_company(company_id: int) -> None:
    """Elimina una empresa del catálogo."""

    _delete_company(company_id)


def ensure_companies(companies: Iterable[Company]) -> List[Company]:
    """Persiste una colección de empresas y regresa sus representaciones actualizadas."""

    return [save_company(company) for company in companies]


def list_incidence_types() -> List[IncidenceType]:
    """Regresa todos los tipos de incidencia registrados."""

    return _list_incidence_types()


def save_incidence_type(entry: IncidenceType) -> IncidenceType:
    """Inserta o actualiza un tipo de incidencia."""

    return _upsert_incidence_type(entry)


def remove_incidence_type(type_id: int) -> None:
    """Elimina un tipo de incidencia del catálogo."""

    _delete_incidence_type(type_id)


__all__ = [
    "Company",
    "IncidenceType",
    "ensure_companies",
    "find_company",
    "list_companies",
    "list_incidence_types",
    "remove_company",
    "remove_incidence_type",
    "save_company",
    "save_incidence_type",
]
