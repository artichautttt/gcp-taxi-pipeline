"""
dbt data catalog: data dictionary and documentation coverage, read from the
dbt artefacts (no BigQuery call, standard library only).

  # artefacts: dbt parse (manifest) and dbt docs generate (catalog, real columns)
  python scripts/dbt_catalog.py dictionary              # writes docs/data_dictionary.md
  python scripts/dbt_catalog.py coverage                # % of documented columns
  python scripts/dbt_catalog.py coverage --fail-under 100

Inputs: dbt/target/manifest.json (required) and dbt/target/catalog.json
(optional). With the catalog, coverage counts the columns that really exist in
BigQuery; without it, or for an object missing from the catalog (ephemeral
model), it counts the columns declared in the schema yml files, and says so.
"""
import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = REPO_ROOT / "dbt" / "target" / "manifest.json"
DEFAULT_CATALOG = REPO_ROOT / "dbt" / "target" / "catalog.json"
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "data_dictionary.md"

DOCUMENTED_TYPES = ("source", "seed", "model")


@dataclass
class Column:
    name: str
    description: str
    data_type: str | None = None
    tests: list[str] = field(default_factory=list)


@dataclass
class DbtObject:
    unique_id: str
    resource_type: str      # source, seed or model
    name: str               # bronze.yellow_tripdata, trips...
    relation: str           # dataset.table
    materialized: str
    path: str
    description: str
    columns: list[Column]
    in_catalog: bool        # columns come from the catalog (real BigQuery columns)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _tests_by_column(manifest: dict) -> dict[tuple[str, str], list[str]]:
    """(node unique_id, lower-case column) -> generic test names, sorted."""
    tests: dict[tuple[str, str], list[str]] = {}
    for node in manifest["nodes"].values():
        meta = node.get("test_metadata")
        if node["resource_type"] != "test" or not meta or not node.get("column_name"):
            continue
        target = node.get("attached_node") or next(iter(node["depends_on"]["nodes"]), "")
        tests.setdefault((target, node["column_name"].lower()), []).append(meta["name"])
    return {key: sorted(names) for key, names in tests.items()}


def collect(manifest: dict, catalog: dict | None) -> list[DbtObject]:
    """Sources, seeds and models of the project, sorted by type then name."""
    cat_entries = {}
    if catalog:
        cat_entries.update(catalog.get("nodes", {}))
        cat_entries.update(catalog.get("sources", {}))
    tests = _tests_by_column(manifest)
    project = manifest["metadata"].get("project_name")

    raw = [n for n in manifest["nodes"].values()
           if n["resource_type"] in ("seed", "model") and n.get("package_name") == project]
    raw += [s for s in manifest["sources"].values() if s.get("package_name") == project]

    objects = []
    for n in raw:
        uid, rtype = n["unique_id"], n["resource_type"]
        yml = {name.lower(): (col.get("description") or "").strip()
               for name, col in n.get("columns", {}).items()}
        cat = cat_entries.get(uid)
        if cat:
            cat_cols = sorted(cat["columns"].values(), key=lambda c: c["index"])
            columns = [Column(c["name"], yml.get(c["name"].lower(), ""), c.get("type"),
                              tests.get((uid, c["name"].lower()), [])) for c in cat_cols]
        else:
            columns = [Column(name, (col.get("description") or "").strip(), col.get("data_type"),
                              tests.get((uid, name.lower()), []))
                       for name, col in n.get("columns", {}).items()]
        if rtype == "source":
            name, relation, materialized = (f"{n['source_name']}.{n['name']}",
                                            f"{n['schema']}.{n['identifier']}", "source")
        else:
            name, relation = n["name"], f"{n['schema']}.{n.get('alias') or n['name']}"
            materialized = n["config"].get("materialized", "")
        objects.append(DbtObject(uid, rtype, name, relation, materialized,
                                 n["original_file_path"].replace("\\", "/"),
                                 (n.get("description") or "").strip(), columns, bool(cat)))
    order = {t: i for i, t in enumerate(DOCUMENTED_TYPES)}
    return sorted(objects, key=lambda o: (order[o.resource_type], o.relation))


@dataclass
class Coverage:
    objects: int = 0
    objects_described: int = 0
    columns: int = 0
    columns_described: int = 0
    catalog_columns: int = 0
    catalog_columns_described: int = 0
    not_in_catalog: list[str] = field(default_factory=list)

    @staticmethod
    def pct(part: int, total: int) -> float | None:
        return round(100 * part / total, 1) if total else None


def coverage(objects: list[DbtObject]) -> Coverage:
    cov = Coverage()
    for o in objects:
        described = sum(1 for c in o.columns if c.description)
        cov.objects += 1
        cov.objects_described += bool(o.description)
        cov.columns += len(o.columns)
        cov.columns_described += described
        if o.in_catalog:
            cov.catalog_columns += len(o.columns)
            cov.catalog_columns_described += described
        else:
            cov.not_in_catalog.append(o.name)
    return cov


def format_coverage(objects: list[DbtObject], cov: Coverage, with_catalog: bool) -> str:
    lines = [f"{'object':40} {'type':7} {'desc':4} {'columns':>7} {'described':>9}  source of columns"]
    for o in objects:
        described = sum(1 for c in o.columns if c.description)
        origin = "catalog" if o.in_catalog else "schema yml"
        lines.append(f"{o.name:40} {o.resource_type:7} {'yes' if o.description else 'no':4} "
                     f"{len(o.columns):>7} {described:>9}  {origin}")
    pct = Coverage.pct
    lines += [
        "",
        f"objects described: {cov.objects_described} / {cov.objects} "
        f"({pct(cov.objects_described, cov.objects)} %)",
        f"columns described: {cov.columns_described} / {cov.columns} "
        f"({pct(cov.columns_described, cov.columns)} %)",
    ]
    if with_catalog:
        lines.append(f"  of which real BigQuery columns (catalog): {cov.catalog_columns_described} / "
                     f"{cov.catalog_columns} ({pct(cov.catalog_columns_described, cov.catalog_columns)} %)")
        if cov.not_in_catalog:
            lines.append(f"  not in the catalog, columns from the schema yml: {', '.join(cov.not_in_catalog)}")
    else:
        lines.append("  no catalog: columns declared in the schema yml only (run dbt docs generate)")
    return "\n".join(lines)


def _cell(text: str | None) -> str:
    return " ".join((text or "").split()).replace("|", "\\|")


LAYER_TITLES = {
    "nyc_taxi": "Chaîne historique (`nyc_taxi`)",
    "nyc_taxi_bronze": "Bronze (`nyc_taxi_bronze`)",
    "nyc_taxi_silver": "Silver (`nyc_taxi_silver`)",
    "nyc_taxi_gold": "Gold (`nyc_taxi_gold`)",
}


def render_dictionary(manifest: dict, objects: list[DbtObject], cov: Coverage,
                      with_catalog: bool) -> str:
    """Markdown data dictionary. Deterministic: no date, so a rerun on the same
    artefacts gives the same file."""
    pct = Coverage.pct
    out = [
        "# Dictionnaire de données (généré)",
        "",
        "Fichier généré par `python scripts/dbt_catalog.py dictionary` à partir du",
        "manifest dbt (`dbt parse`)" + (" et du catalog (`dbt docs generate`, colonnes et "
                                         "types réels de BigQuery)." if with_catalog else
                                         " ; sans catalog, les colonnes sont celles des yml."),
        "Ne pas modifier à la main : les descriptions vivent dans les `schema.yml`",
        "et les doc blocks de `dbt/models/_docs.md`.",
        "",
        f"- Objets : {cov.objects} (sources, seeds, modèles), "
        f"{cov.objects_described} décrits",
        f"- Colonnes décrites : {cov.columns_described} / {cov.columns} "
        f"({pct(cov.columns_described, cov.columns)} %)",
    ]
    if with_catalog and cov.not_in_catalog:
        out.append(f"- Hors catalog dbt (tables wildcard, modèle éphémère) : colonnes lues "
                   f"dans les yml, sans type : {', '.join(f'`{n}`' for n in cov.not_in_catalog)}")
    out.append("")

    by_layer: dict[str, list[DbtObject]] = {}
    for o in objects:
        by_layer.setdefault(o.relation.split(".")[0], []).append(o)
    layers = [d for d in LAYER_TITLES if d in by_layer] + sorted(set(by_layer) - set(LAYER_TITLES))
    for dataset in layers:
        out += [f"## {LAYER_TITLES.get(dataset, f'`{dataset}`')}", ""]
        for o in by_layer[dataset]:
            kind = o.resource_type if o.resource_type == "source" else \
                f"{o.resource_type} ({o.materialized})"
            out += [f"### `{o.relation}`", "", f"{kind} `{o.name}`, `{o.path}`", ""]
            if o.description:
                out += [o.description, ""]
            if o.columns:
                out += ["| Colonne | Type | Description | Tests |", "|---|---|---|---|"]
                out += [f"| `{c.name}` | {_cell(c.data_type)} | {_cell(c.description)} | "
                        f"{', '.join(c.tests)} |" for c in o.columns]
                out.append("")

    project = manifest["metadata"].get("project_name")
    blocks = sorted((d for d in manifest.get("docs", {}).values()
                     if d.get("package_name") == project), key=lambda d: d["name"])
    if blocks:
        out += ["## Doc blocks (`dbt/models/_docs.md`)", ""]
        for d in blocks:
            out += [f"### `{d['name']}`", "", d["block_contents"].strip(), ""]
    return "\n".join(out).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("command", choices=["dictionary", "coverage"])
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG,
                        help="optional; ignored when the file does not exist")
    parser.add_argument("--no-catalog", action="store_true", help="ignore the catalog")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--fail-under", type=float,
                        help="coverage: exit 1 when the %% of described columns is lower")
    args = parser.parse_args(argv)

    manifest = load_json(args.manifest)
    catalog = None if args.no_catalog or not args.catalog.exists() else load_json(args.catalog)
    objects = collect(manifest, catalog)
    cov = coverage(objects)

    if args.command == "dictionary":
        args.output.write_text(render_dictionary(manifest, objects, cov, catalog is not None),
                               encoding="utf-8", newline="\n")
        print(f"wrote {args.output} ({cov.objects} objects, {cov.columns} columns)")
        return 0

    print(format_coverage(objects, cov, catalog is not None))
    pct = Coverage.pct(cov.columns_described, cov.columns) or 0.0
    if args.fail_under is not None and pct < args.fail_under:
        print(f"FAIL: {pct} % of columns described, below {args.fail_under} %")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
