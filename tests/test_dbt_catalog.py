"""
Tests for scripts/dbt_catalog.py on small hand-written dbt artefacts
(no dbt, no BigQuery).
"""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "dbt_catalog.py"

spec = importlib.util.spec_from_file_location("dbt_catalog", SCRIPT)
dbt_catalog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dbt_catalog)


def model(name, columns, description="", materialized="table", package="proj"):
    return {
        "unique_id": f"model.{package}.{name}", "resource_type": "model", "name": name,
        "package_name": package, "schema": "silver", "alias": name,
        "config": {"materialized": materialized},
        "original_file_path": f"models\\silver\\{name}.sql", "description": description,
        "columns": {c: {"name": c, "description": d} for c, d in columns.items()},
    }


def manifest():
    test = {
        "unique_id": "test.proj.not_null_trips_trip_id", "resource_type": "test",
        "package_name": "proj", "column_name": "trip_id",
        "attached_node": "model.proj.trips", "test_metadata": {"name": "not_null"},
        "depends_on": {"nodes": ["model.proj.trips"]},
    }
    singular = {  # singular test: no test_metadata, never attached to a column
        "unique_id": "test.proj.silver_plus_quarantine", "resource_type": "test",
        "package_name": "proj", "depends_on": {"nodes": ["model.proj.trips"]},
    }
    return {
        "metadata": {"project_name": "proj"},
        "nodes": {
            "model.proj.trips": model("trips", {"trip_id": "Fingerprint | hash", "fare": ""},
                                      description="Silver trips."),
            "model.proj.int_x": model("int_x", {"a": "A column"}, materialized="ephemeral"),
            "model.other.ignored": model("ignored", {"z": ""}, package="other"),
            test["unique_id"]: test,
            singular["unique_id"]: singular,
        },
        "sources": {
            "source.proj.bronze.raw": {
                "unique_id": "source.proj.bronze.raw", "resource_type": "source",
                "package_name": "proj", "source_name": "bronze", "name": "raw",
                "schema": "bronze", "identifier": "raw_*",
                "original_file_path": "models/silver/_bronze__sources.yml",
                "description": "", "columns": {},
            }
        },
        "docs": {
            "doc.proj.quarantine": {"name": "quarantine", "package_name": "proj",
                                    "block_contents": "\nRejected rows.\n"},
            "doc.dbt.__overview__": {"name": "__overview__", "package_name": "dbt",
                                     "block_contents": "dbt default"},
        },
    }


def catalog():
    cols = {"TRIP_ID": ("INT64", 1), "fare": ("FLOAT64", 2), "undeclared": ("STRING", 3)}
    return {"nodes": {"model.proj.trips": {"columns": {
        n: {"name": n, "type": t, "index": i} for n, (t, i) in cols.items()}}}, "sources": {}}


class CollectTest(unittest.TestCase):
    def test_catalog_columns_replace_yml_columns(self):
        objects = {o.name: o for o in dbt_catalog.collect(manifest(), catalog())}
        trips = objects["trips"]
        self.assertTrue(trips.in_catalog)
        self.assertEqual([c.name for c in trips.columns], ["TRIP_ID", "fare", "undeclared"])
        # the yml description is matched case-insensitively, the type comes from the catalog
        self.assertEqual(trips.columns[0].description, "Fingerprint | hash")
        self.assertEqual(trips.columns[0].data_type, "INT64")
        self.assertEqual(trips.columns[0].tests, ["not_null"])
        self.assertEqual(trips.columns[2].description, "")

    def test_objects_outside_the_project_are_ignored(self):
        names = [o.name for o in dbt_catalog.collect(manifest(), catalog())]
        self.assertNotIn("ignored", names)
        # sources first, then models
        self.assertEqual(names[0], "bronze.raw")

    def test_object_missing_from_catalog_uses_yml_columns(self):
        objects = {o.name: o for o in dbt_catalog.collect(manifest(), catalog())}
        self.assertFalse(objects["int_x"].in_catalog)
        self.assertEqual([c.name for c in objects["int_x"].columns], ["a"])
        self.assertEqual(objects["bronze.raw"].relation, "bronze.raw_*")


class CoverageTest(unittest.TestCase):
    def test_counts(self):
        cov = dbt_catalog.coverage(dbt_catalog.collect(manifest(), catalog()))
        self.assertEqual((cov.objects, cov.objects_described), (3, 1))
        # trips: 3 real columns, 1 described; int_x: 1 yml column, described
        self.assertEqual((cov.columns, cov.columns_described), (4, 2))
        self.assertEqual((cov.catalog_columns, cov.catalog_columns_described), (3, 1))
        self.assertEqual(cov.not_in_catalog, ["bronze.raw", "int_x"])

    def test_without_catalog_only_yml_columns_count(self):
        cov = dbt_catalog.coverage(dbt_catalog.collect(manifest(), None))
        self.assertEqual((cov.columns, cov.columns_described), (3, 2))
        self.assertEqual(cov.catalog_columns, 0)

    def test_pct_of_nothing_is_none(self):
        self.assertIsNone(dbt_catalog.Coverage.pct(0, 0))
        self.assertEqual(dbt_catalog.Coverage.pct(1, 3), 33.3)


class DictionaryTest(unittest.TestCase):
    def render(self):
        objects = dbt_catalog.collect(manifest(), catalog())
        return dbt_catalog.render_dictionary(manifest(), objects,
                                             dbt_catalog.coverage(objects), True)

    def test_content(self):
        text = self.render()
        self.assertIn("### `silver.trips`", text)
        self.assertIn("| `TRIP_ID` | INT64 | Fingerprint \\| hash | not_null |", text)
        self.assertIn("### `quarantine`\n\nRejected rows.", text)
        self.assertNotIn("dbt default", text)
        self.assertIn("Colonnes décrites : 2 / 4 (50.0 %)", text)

    def test_deterministic(self):
        self.assertEqual(self.render(), self.render())


class MainTest(unittest.TestCase):
    def run_main(self, *args):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "manifest.json").write_text(json.dumps(manifest()), encoding="utf-8")
            (tmp / "catalog.json").write_text(json.dumps(catalog()), encoding="utf-8")
            code = dbt_catalog.main([*args, "--manifest", str(tmp / "manifest.json"),
                                     "--catalog", str(tmp / "catalog.json"),
                                     "--output", str(tmp / "dict.md")])
            written = (tmp / "dict.md").read_text(encoding="utf-8") if (tmp / "dict.md").exists() else None
        return code, written

    def test_fail_under(self):
        self.assertEqual(self.run_main("coverage", "--fail-under", "50")[0], 0)
        self.assertEqual(self.run_main("coverage", "--fail-under", "50.1")[0], 1)

    def test_dictionary_writes_file(self):
        code, written = self.run_main("dictionary")
        self.assertEqual(code, 0)
        self.assertTrue(written.startswith("# Dictionnaire de données (généré)"))

    def test_missing_catalog_is_optional(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "manifest.json").write_text(json.dumps(manifest()), encoding="utf-8")
            code = dbt_catalog.main(["coverage", "--manifest", str(tmp / "manifest.json"),
                                     "--catalog", str(tmp / "absent.json")])
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
