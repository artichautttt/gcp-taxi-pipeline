"""
Tests for scripts/check_secrets.py (stdlib unittest, no extra dependency):

  python -m unittest discover -s tests -v

Fake credentials are assembled at runtime so this file itself never
contains a string the scanner would flag.
"""
import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "check_secrets.py"

spec = importlib.util.spec_from_file_location("check_secrets", SCRIPT)
check_secrets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_secrets)

Q = '"'
FAKE_SA_KEY = "{" + f"{Q}type{Q}: {Q}service{'_'}account{Q}, {Q}private_key_id{Q}: {Q}{'ab12' * 10}{Q}" + "}"
FAKE_PEM = "-----BEGIN " + "PRIVATE KEY-----\nMIIEv...\n-----END " + "PRIVATE KEY-----"
FAKE_API_KEY = "AI" + "za" + "B" * 35
FAKE_REFRESH = "{" + f"{Q}refresh_token{Q}: {Q}1//{'x' * 40}{Q}" + "}"


class ScanTextTest(unittest.TestCase):
    def names(self, text):
        return {name for _, name in check_secrets.scan_text(text)}

    def test_detects_service_account_key_json(self):
        self.assertEqual(self.names(FAKE_SA_KEY), {"GCP service account key", "GCP private key id"})

    def test_detects_pem_private_key(self):
        self.assertIn("PEM private key", self.names(FAKE_PEM))

    def test_detects_google_api_key(self):
        self.assertIn("Google API key", self.names(f"KEY={FAKE_API_KEY}"))

    def test_detects_adc_refresh_token(self):
        self.assertIn("ADC user refresh token", self.names(FAKE_REFRESH))

    def test_reports_line_number(self):
        self.assertEqual(check_secrets.scan_text(f"a\nb\n{FAKE_API_KEY}\n"), [(3, "Google API key")])

    def test_ignores_legit_config(self):
        legit = ("GCP_PROJECT_ID=mini-siem-taxi-pipeline\nBQ_DATASET=nyc_taxi\n"
                 "gcloud auth application-default login --impersonate-service-account=x@y.iam.gserviceaccount.com\n"
                 "type: service_account_impersonation\n")
        self.assertEqual(check_secrets.scan_text(legit), [])


class ForbiddenFilesTest(unittest.TestCase):
    def test_forbidden_paths(self):
        for path in [".env", "service-account-key.json", "conf/.env", "application_default_credentials.json"]:
            self.assertTrue(check_secrets.FORBIDDEN_FILES.search(path), path)

    def test_allowed_paths(self):
        for path in [".env.example", "iam/iam_policy.yaml", "requirements.txt"]:
            self.assertFalse(check_secrets.FORBIDDEN_FILES.search(path), path)


class RepositoryTest(unittest.TestCase):
    def test_repository_has_no_committed_credentials(self):
        """The real gate: fails if a credential is tracked in this repo."""
        result = subprocess.run([sys.executable, str(SCRIPT), "--history"], cwd=REPO_ROOT,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
