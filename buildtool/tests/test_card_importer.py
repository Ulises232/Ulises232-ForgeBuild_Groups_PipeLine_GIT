import os
import tempfile
import unittest
from pathlib import Path

from buildtool.core import card_importer
from buildtool.core.card_importer import (
    CardImportEntry,
    CardImportError,
    apply_card_entries,
)
from buildtool.core.branch_history_db import Card, Company, IncidenceType


class CardImporterTests(unittest.TestCase):
    def test_load_card_entries_from_csv(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", suffix=".csv", delete=False) as handle:
            handle.write(
                "Grupo,Empresa,Ticket,Titulo,Desarrollador (opcional),QA(opcional)\n"
                "Alpha,Acme,ABC-1,Primera,,qa1\n"
                ",,,,,\n"
            )
            path = Path(handle.name)

        try:
            entries, skipped = card_importer.load_card_entries(path)
        finally:
            os.unlink(path)

        self.assertEqual(skipped, 1)
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.group_name, "Alpha")
        self.assertEqual(entry.company_name, "Acme")
        self.assertEqual(entry.ticket_id, "ABC-1")
        self.assertEqual(entry.title, "Primera")
        self.assertIsNone(entry.assignee)
        self.assertEqual(entry.qa_assignee, "qa1")
        self.assertIsNone(entry.incidence_type_name)
        self.assertFalse(entry.incidence_type_provided)

    def test_load_card_entries_missing_column(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="", suffix=".csv", delete=False) as handle:
            handle.write("Grupo,Ticket,Titulo\nAlpha,ABC-1,Primera\n")
            path = Path(handle.name)

        try:
            with self.assertRaises(CardImportError):
                card_importer.load_card_entries(path)
        finally:
            os.unlink(path)

    def test_load_card_entries_from_cp1252_csv(self) -> None:
        content = (
            "Grupo;Empresa;Ticket;Título;Desarrollador (opcional);QA (opcional)\r\n"
            "Álpha;Acmé;ABC-5;Título con acento;dév;qa\r\n"
        )
        with tempfile.NamedTemporaryFile("wb", suffix=".csv", delete=False) as handle:
            handle.write(content.encode("cp1252"))
            path = Path(handle.name)

        try:
            entries, skipped = card_importer.load_card_entries(path)
        finally:
            os.unlink(path)

        self.assertEqual(skipped, 0)
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.group_name, "Álpha")
        self.assertEqual(entry.company_name, "Acmé")
        self.assertEqual(entry.ticket_id, "ABC-5")
        self.assertEqual(entry.title, "Título con acento")
        self.assertEqual(entry.assignee, "dév")
        self.assertEqual(entry.qa_assignee, "qa")
        self.assertIsNone(entry.incidence_type_name)
        self.assertFalse(entry.incidence_type_provided)

    def test_apply_card_entries_creates_and_updates(self) -> None:
        entries = [
            CardImportEntry(
                row=2,
                group_name="Alpha",
                company_name="Acme",
                ticket_id="ABC-1",
                title="Primera tarjeta",
                assignee="dev1",
                incidence_type_name="Bug",
                incidence_type_provided=True,
            ),
            CardImportEntry(
                row=3,
                group_name="Alpha",
                company_name="Acme",
                ticket_id="ABC-2",
                title="Tarjeta actualizada",
                qa_assignee="qa1",
            ),
        ]

        existing = [
            Card(
                id=7,
                sprint_id=None,
                branch_key=None,
                title="Original",
                ticket_id="ABC-2",
                branch="",
                group_name="Alpha",
                assignee=None,
                qa_assignee=None,
                description="",
                unit_tests_url=None,
                qa_url=None,
                unit_tests_done=False,
                qa_done=False,
                unit_tests_by=None,
                qa_by=None,
                unit_tests_at=None,
                qa_at=None,
                status="pending",
                company_id=10,
                closed_at=None,
                closed_by=None,
                branch_created_by=None,
                branch_created_at=None,
                created_at=0,
                created_by="",
                updated_at=0,
                updated_by="",
            )
        ]

        saved: list[Card] = []

        def list_cards_stub() -> list[Card]:
            return list(existing)

        def upsert_stub(card: Card) -> Card:
            saved.append(card)
            if card.id is None:
                card.id = 100 + len(saved)
            return card

        companies = [Company(id=10, name="Acme", group_name="Alpha")]

        types = [IncidenceType(id=5, name="Bug")]

        summary = apply_card_entries(
            entries,
            username="tester",
            skipped_rows=1,
            list_cards_fn=list_cards_stub,
            upsert_card_fn=upsert_stub,
            list_companies_fn=lambda: companies,
            list_incidence_types_fn=lambda: types,
        )

        self.assertEqual(summary.created, 1)
        self.assertEqual(summary.updated, 1)
        self.assertEqual(summary.skipped, 1)
        self.assertFalse(summary.errors)

        self.assertEqual(len(saved), 2)
        created = saved[0]
        self.assertEqual(created.ticket_id, "ABC-1")
        self.assertEqual(created.group_name, "Alpha")
        self.assertEqual(created.company_id, 10)
        self.assertEqual(created.assignee, "dev1")
        self.assertEqual(created.created_by, "tester")
        self.assertEqual(created.incidence_type_id, 5)

        updated = saved[1]
        self.assertEqual(updated.id, 7)
        self.assertEqual(updated.title, "Tarjeta actualizada")
        self.assertEqual(updated.qa_assignee, "qa1")
        self.assertEqual(updated.updated_by, "tester")

    def test_apply_card_entries_reports_missing_company(self) -> None:
        entries = [
            CardImportEntry(
                row=4,
                group_name="Alpha",
                company_name="Desconocida",
                ticket_id="ABC-3",
                title="Tarjeta sin empresa",
            )
        ]

        summary = apply_card_entries(
            entries,
            username="tester",
            list_cards_fn=lambda: [],
            upsert_card_fn=lambda card: card,
            list_companies_fn=lambda: [Company(id=10, name="Acme", group_name="Alpha")],
            list_incidence_types_fn=lambda: [],
        )

        self.assertEqual(summary.created, 0)
        self.assertEqual(summary.updated, 0)
        self.assertEqual(summary.skipped, 1)
        self.assertEqual(len(summary.errors), 1)
        row, message = summary.errors[0]
        self.assertEqual(row, 4)
        self.assertIn("empresa", message.lower())


if __name__ == "__main__":  # pragma: no cover - permite ejecución directa
    unittest.main()

