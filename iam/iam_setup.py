"""
Least-privilege IAM setup for the taxi pipeline (see iam/iam_policy.yaml).

  python iam/iam_setup.py audit   # read-only: which identity runs the pipeline,
                                  # which roles it has, is it more than needed?
  python iam/iam_setup.py plan    # print the gcloud / BigQuery changes, change nothing
  python iam/iam_setup.py apply --operator you@example.com
                                  # create the SA + custom role, grant the bindings

`audit` exits with status 1 if the identity used by the pipeline holds a
project admin role (owner/editor), so it can be used as a manual/CI gate.

Requires gcloud CLI authenticated as a project owner for `apply`, and the
same python env as the pipeline (google-cloud-bigquery) for dataset access.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

IAM_DIR = Path(__file__).resolve().parent
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "mini-siem-taxi-pipeline")
DATASET = os.environ.get("BQ_DATASET", "nyc_taxi")
# Medallion datasets, created once by the project owner (the SA cannot create
# datasets). The SA gets WRITER on these and on DATASET, nothing else.
MEDALLION_DATASETS = [
    os.environ.get("BQ_BRONZE_DATASET", "nyc_taxi_bronze"),
    os.environ.get("BQ_SILVER_DATASET", "nyc_taxi_silver"),
    os.environ.get("BQ_GOLD_DATASET", "nyc_taxi_gold"),
]
PIPELINE_DATASETS = [DATASET, *MEDALLION_DATASETS]
SA_ID = "taxi-pipeline"
SA_EMAIL = f"{SA_ID}@{PROJECT_ID}.iam.gserviceaccount.com"
ROLE_ID = "taxiPipelineJobRunner"
ROLE_NAME = f"projects/{PROJECT_ID}/roles/{ROLE_ID}"

# Roles that give far more than the pipeline needs (IAM changes, deleting
# datasets, access to every dataset of the project...).
ADMIN_ROLES = {"roles/owner", "roles/editor", "roles/bigquery.admin"}
# Dataset ACL entries that allow more than read/write of tables.
DATASET_ADMIN_ACL = {"OWNER", "roles/bigquery.dataOwner"}


def gcloud(*args: str, capture: bool = True) -> str:
    exe = shutil.which("gcloud")
    if exe is None:
        sys.exit("[error] gcloud CLI not found in PATH")
    result = subprocess.run([exe, *args], capture_output=capture, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gcloud {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout if capture else ""


def adc_identity() -> tuple[str, str]:
    """Returns (credential type, principal) of the Application Default Credentials."""
    import google.auth
    import google.auth.transport.requests
    import requests

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    target = getattr(creds, "_target_principal", None)  # impersonated credentials
    if target:
        return "impersonated_service_account", f"serviceAccount:{target}"
    if hasattr(creds, "service_account_email") and creds.service_account_email not in (None, "default"):
        return "service_account", f"serviceAccount:{creds.service_account_email}"

    # authorized_user: the file holds no email, ask the token info endpoint.
    creds.refresh(google.auth.transport.requests.Request())
    info = requests.get("https://oauth2.googleapis.com/tokeninfo",
                        params={"access_token": creds.token}, timeout=30).json()
    email = info.get("email")
    return "authorized_user", f"user:{email}" if email else "user:<unknown>"


def project_roles_of(member: str) -> list[str]:
    policy = json.loads(gcloud("projects", "get-iam-policy", PROJECT_ID, "--format=json"))
    return sorted(b["role"] for b in policy.get("bindings", []) if member in b.get("members", []))


def dataset_acl_of(member: str, client=None) -> dict[str, list[str]]:
    """ACL roles of `member` on each pipeline dataset ('missing' if the dataset does not exist)."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    client = client or bigquery.Client(project=PROJECT_ID)
    email = member.split(":", 1)[1]
    roles = {}
    for name in PIPELINE_DATASETS:
        try:
            dataset = client.get_dataset(name)
        except NotFound:
            roles[name] = ["missing"]
            continue
        roles[name] = sorted(e.role for e in dataset.access_entries if e.entity_id == email)
    return roles


def audit() -> int:
    cred_type, member = adc_identity()
    project_roles = project_roles_of(member)
    dataset_roles = dataset_acl_of(member)
    sa_list = json.loads(gcloud("iam", "service-accounts", "list",
                                f"--project={PROJECT_ID}", "--format=json"))

    print(f"[audit] project           : {PROJECT_ID}")
    print(f"[audit] ADC credential    : {cred_type}")
    print(f"[audit] pipeline identity : {member}")
    print(f"[audit] project roles     : {project_roles or '-'}")
    for name, roles in dataset_roles.items():
        print(f"[audit] {name + ' ACL':<18}: {roles or '-'} "
              "(+ inherited via projectOwners/projectWriters special groups)")
    print(f"[audit] service accounts  : {[sa['email'] for sa in sa_list] or 'none'}")

    findings = []
    admin = sorted(set(project_roles) & ADMIN_ROLES)
    if admin:
        findings.append(f"pipeline identity holds project admin role(s) {admin}: "
                        "can change IAM, delete any dataset, read every dataset")
    for name, roles in dataset_roles.items():
        if set(roles) & DATASET_ADMIN_ACL:
            findings.append(f"pipeline identity is OWNER of dataset {name}: "
                            "can change its access list and delete it")
    if cred_type == "service_account":
        findings.append("ADC uses a service account KEY file: long-lived secret on disk, "
                        "prefer impersonation (see README)")

    if not findings:
        print("[audit] OK: pipeline identity has no admin role")
        return 0
    for f in findings:
        print(f"[audit] FINDING: {f}")
    return 1


def steps(operator: str) -> list[tuple[str, list[str]]]:
    """(description, gcloud args) for each change, in order. Dataset ACL handled separately."""
    return [
        ("enable IAM APIs (needed for SA + impersonation)",
         ["services", "enable", "iam.googleapis.com", "iamcredentials.googleapis.com",
          f"--project={PROJECT_ID}"]),
        ("create custom role (bigquery.jobs.create only)",
         ["iam", "roles", "create", ROLE_ID, f"--project={PROJECT_ID}",
          f"--file={IAM_DIR / 'custom_role_job_runner.yaml'}"]),
        ("create dedicated service account (no key)",
         ["iam", "service-accounts", "create", SA_ID, f"--project={PROJECT_ID}",
          "--display-name=Taxi pipeline (least privilege)"]),
        ("grant custom role on the project",
         ["projects", "add-iam-policy-binding", PROJECT_ID,
          f"--member=serviceAccount:{SA_EMAIL}", f"--role={ROLE_NAME}", "--condition=None"]),
        ("let the operator impersonate this SA only",
         ["iam", "service-accounts", "add-iam-policy-binding", SA_EMAIL,
          f"--project={PROJECT_ID}", f"--member=user:{operator}",
          "--role=roles/iam.serviceAccountTokenCreator"]),
    ]


def grant_dataset_writer(client=None, names=None) -> list[str]:
    """Adds WRITER for the SA on each pipeline dataset (idempotent).
    Returns the datasets actually changed."""
    from google.cloud import bigquery

    client = client or bigquery.Client(project=PROJECT_ID)
    changed = []
    for name in names or PIPELINE_DATASETS:
        dataset = client.get_dataset(name)
        entry = bigquery.AccessEntry(role="WRITER", entity_type="userByEmail", entity_id=SA_EMAIL)
        if entry in dataset.access_entries:
            print(f"[skip] {SA_EMAIL} already WRITER on {name}")
            continue
        dataset.access_entries = [*dataset.access_entries, entry]
        client.update_dataset(dataset, ["access_entries"])
        changed.append(name)
        print(f"[done] {SA_EMAIL} WRITER (= roles/bigquery.dataEditor) on {PROJECT_ID}:{name}")
    return changed


def plan(operator: str) -> None:
    for desc, args in steps(operator):
        shown = [f'"{a}"' if " " in a else a for a in args]
        print(f"# {desc}\ngcloud {' '.join(shown)}\n")
    print(f"# grant read/write on datasets {', '.join(PIPELINE_DATASETS)} only "
          "(dataset access lists, via Python client)")
    print(f"#   AccessEntry(role='WRITER', entity_type='userByEmail', entity_id='{SA_EMAIL}')\n")
    print("# then point ADC at the SA (short-lived tokens, no key file):")
    print(f"gcloud auth application-default login --impersonate-service-account={SA_EMAIL}")


def apply(operator: str) -> None:
    for desc, args in steps(operator):
        print(f"[apply] {desc}")
        try:
            gcloud(*args)
        except RuntimeError as e:
            if "already exists" in str(e):
                print("  [skip] already exists")
            else:
                raise
    grant_dataset_writer()
    print("\nNext: gcloud auth application-default login "
          f"--impersonate-service-account={SA_EMAIL}\n"
          "then re-run `python iam/iam_setup.py audit` (expected: OK).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Least-privilege IAM for the taxi pipeline")
    parser.add_argument("action", choices=["audit", "plan", "apply"])
    parser.add_argument("--operator", default="<OPERATOR_EMAIL>",
                        help="human account allowed to impersonate the SA (plan/apply)")
    args = parser.parse_args()

    if args.action == "audit":
        sys.exit(audit())
    if args.action == "plan":
        plan(args.operator)
    else:
        if args.operator.startswith("<"):
            sys.exit("[error] --operator <email> is required for apply")
        apply(args.operator)
