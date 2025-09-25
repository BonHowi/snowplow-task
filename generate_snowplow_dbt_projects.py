#!/usr/bin/env python3
"""
generate_snowplow_dbt_projects.py

This version requires `ruamel.yaml` to be installed:
    pip install ruamel.yaml

Usage:
  # Single JSON:
  python generate_snowplow_dbt_projects.py --input brand.json

  # Directory of JSONs:
  python generate_snowplow_dbt_projects.py --input-dir ./brands_json
"""

from pathlib import Path
import argparse
import json
import datetime
import re
import sys
import shutil
from typing import Dict, Any, List
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dqs


# Defaults for package location
DEFAULT_PACKAGE_GIT = "https://github.com/snowplow/snowplow-unified-dbt.git"
DEFAULT_PACKAGE_REF = "main"

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False  # block style by default
yaml.preserve_quotes = True


def slugify(name: str) -> str:
    """Convert a string into a lowercase, dash-separated slug."""
    s = name.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = re.sub(r"-+", "-", s)
    return s


def make_inline_seq(items: List[Any]) -> CommentedSeq:
    """Return a CommentedSeq with inline [...] style for ruamel.yaml."""
    seq = CommentedSeq(items)
    seq.fa.set_flow_style()
    return seq


def dump_yaml(obj: Any, path: Path):
    """Dump object to YAML using ruamel.yaml."""
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(obj, f)
    print(f"wrote: {path}")


def build_snowplow_vars(customer_json: Dict[str, Any]) -> CommentedMap:
    """Build Snowplow vars for dbt_project.yml."""
    vars_block = CommentedMap()
    user_vars = customer_json.get("user_set_variables", {})
    if isinstance(user_vars, dict):
        vars_block.update(user_vars)

    start_date = customer_json.get("historical_data_since")
    if start_date:
        vars_block["snowplow__start_date"] = start_date

    vars_block["snowplow__enable_mobile_data"] = customer_json.get("mobile_tracking", "").lower() == "yes"
    vars_block["snowplow__enable_web_data"] = customer_json.get("web_tracking", "").lower() == "yes"

    app_ids = customer_json.get("app_ids")
    if app_ids:
        vars_block["snowplow__app_ids"] = app_ids

    vars_block["snowplow__brand_name"] = customer_json.get("brand_name")
    return vars_block


def write_file(path: Path, content: str):
    """Write content to a file, creating parent directories if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"wrote: {path}")


def handle_project_dir(project_dir: Path):
    """Handle existing per-brand project directory."""
    if project_dir.exists():
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        response = input(f"Project directory '{project_dir}' already exists. Override? [y/N]: ").strip().lower()
        if response != "y":
            new_name = project_dir.parent / f"{project_dir.name}_old_{now}"
            shutil.move(project_dir, new_name)
            print(f"Existing project renamed to '{new_name}'")
        else:
            shutil.rmtree(project_dir)
            print(f"Existing project '{project_dir}' removed for override.")


def generate_project_for_customer(
    customer_json: Dict[str, Any],
    out_root: Path,
    package_git: str = DEFAULT_PACKAGE_GIT,
    package_ref: str = DEFAULT_PACKAGE_REF
) -> Path:
    """Generate a dbt project for a specific customer using ruamel.yaml."""
    brand = customer_json.get("brand_name", "unnamed_brand")
    slug = slugify(brand)
    project_dir = out_root / f"dbt_{slug}"

    handle_project_dir(project_dir)
    project_dir.mkdir(parents=True, exist_ok=True)

    project_name = f"snowplow_unified_for_{slug}"
    now = datetime.date.today().isoformat()

    # packages.yml
    packages_obj = CommentedMap({"packages": [CommentedMap({"git": package_git, "revision": package_ref})]})
    dump_yaml(packages_obj, project_dir / "packages.yml")

    # dbt_project.yml
    snowplow_vars = build_snowplow_vars(customer_json)
    dbt_project_obj = CommentedMap()
    dbt_project_obj["name"] = dqs(project_name)
    dbt_project_obj["version"] = "1.0.0"
    dbt_project_obj["config-version"] = 2
    dbt_project_obj["require-dbt-version"] = make_inline_seq([dqs(">=1.6.0"), dqs("<2.0.0")])
    dbt_project_obj["profile"] = "your_profile_name_here"
    dbt_project_obj["dispatch"] = [
        CommentedMap([("macro_namespace", "dbt"), ("search_order", make_inline_seq([dqs("snowplow_utils"), dqs("dbt")]))])
    ]
    dbt_project_obj["model-paths"] = make_inline_seq(["models"])
    dbt_project_obj["analysis-paths"] = make_inline_seq(["analysis"])
    dbt_project_obj["test-paths"] = make_inline_seq(["tests"])
    dbt_project_obj["macro-paths"] = make_inline_seq(["macros"])
    dbt_project_obj["docs-paths"] = make_inline_seq(["docs"])
    dbt_project_obj["asset-paths"] = make_inline_seq(["assets"])
    dbt_project_obj["target-path"] = "target"
    dbt_project_obj["clean-targets"] = make_inline_seq(["target", "dbt_modules", "dbt_packages"])
    dbt_project_obj["vars"] = CommentedMap({"snowplow_unified": snowplow_vars})

    # models
    dbt_project_obj["models"] = CommentedMap({
        "snowplow_unified": CommentedMap({
            "base": CommentedMap({
                "manifest": CommentedMap({"+schema": "my_manifest_schema"}),
                "scratch": CommentedMap({"+schema": "my_scratch_schema"})
            }),
            "sessions": CommentedMap({
                "+schema": "my_derived_schema",
                "scratch": CommentedMap({"+schema": "my_scratch_schema"})
            })
        })
    })

    dump_yaml(dbt_project_obj, project_dir / "dbt_project.yml")

    # profiles.example.yml
    profiles_example = """# Minimal example profile. Replace values with your warehouse config.
your_profile_name_here:
  target: dev
  outputs:
    dev:
      type: <your_warehouse>  # e.g., bigquery, snowflake, redshift, databricks
      threads: 1
      # other warehouse-specific connection properties here
"""
    write_file(project_dir / "profiles.example.yml", profiles_example)

    # README
    readme = f"# {project_name}\n\nGenerated on {now} from brand: {brand}\n"
    write_file(project_dir / "README.md", readme)

    # Ensure models dir exists
    (project_dir / "models").mkdir(exist_ok=True)

    print(f"Project for '{brand}' generated at: {project_dir}")
    return project_dir


def load_json_file(path: Path) -> Dict[str, Any]:
    """Load a JSON file into a Python dictionary."""
    if not path.exists():
        print(f"ERROR: Input file '{path}' does not exist.", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Generate per-customer dbt projects for Snowplow Unified.")
    parser.add_argument("--input", "-i", type=str, help="One input JSON file")
    parser.add_argument("--input-dir", "-I", type=str, help="Directory with JSON files")
    parser.add_argument("--out", "-o", type=str, default="./dbt_projects", help="Output root dir")
    parser.add_argument("--package-git", type=str, default=DEFAULT_PACKAGE_GIT,
                        help="Snowplow unified dbt package git URL")
    parser.add_argument("--package-ref", type=str, default=DEFAULT_PACKAGE_REF, help="Git ref/branch/tag")
    args = parser.parse_args()

    out_root = Path(args.out)

    inputs: List[Path] = []
    if args.input:
        inputs.append(Path(args.input))
    if args.input_dir:
        inputs.extend(sorted(Path(args.input_dir).glob("*.json")))

    if not inputs:
        print("No input files provided.", file=sys.stderr)
        sys.exit(2)

    for ip in inputs:
        print(f"Processing {ip}...")
        customer_json = load_json_file(ip)
        generate_project_for_customer(customer_json, out_root,
                                      package_git=args.package_git, package_ref=args.package_ref)


if __name__ == "__main__":
    main()
