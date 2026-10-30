#!/usr/bin/env python3

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


class UnityCatalogCheckoutTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = self.root / "upstream"
        self.checkout = self.root / "checkout"
        self.git("init", "--bare", "--quiet", str(self.repo), cwd=self.root)
        self.git("config", "user.name", "Test User")
        self.git("config", "user.email", "test@example.com")
        self.git("symbolic-ref", "HEAD", "refs/heads/main")
        blob = self.git(
            "hash-object",
            "-w",
            "--stdin",
            input='ThisBuild / version := "0.8.0-SNAPSHOT"\n',
        )
        tree = self.git("mktree", input=f"100644 blob {blob}\tversion.sbt\n")
        self.main = self.git("commit-tree", tree, input="Main\n")
        self.pin = self.git(
            "commit-tree", tree, "-p", self.main, input="Pull request\n"
        )
        self.git("update-ref", "refs/heads/main", self.main)
        self.git("update-ref", "refs/pull/2093/head", self.pin)
        self.git("update-ref", "refs/tags/test-release", self.pin)

    def git(self, *args, cwd=None, input=None):
        return subprocess.run(
            ["git", *args],
            cwd=cwd or self.repo,
            input=input,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    def run_checkout(self, pin, ref, uc_ref=None, base_version="0.8.0-SNAPSHOT"):
        script = (
            Path(__file__).parents[1] / "scripts" / "setup_unitycatalog_main.sh"
        ).read_text()
        # Run the real checkout and version checks without publishing or clearing artifact caches.
        script = script[: script.index("\n# Override version.sbt")]
        for name, value in (
            ("UC_PIN_SHA", pin),
            ("UC_PIN_REF", ref),
            ("UC_BASE_VERSION", base_version),
        ):
            script = re.sub(
                f"^{name}=.*$", f"{name}={value}", script, flags=re.MULTILINE
            )
        env = dict(
            os.environ,
            UC_REPO=str(self.repo),
            UC_DIR=str(self.checkout),
            UC_FORCE="1",
            DELTA_RELEASE_MODE="0",
        )
        env.pop("UC_VERSION", None)
        env.pop("UC_REF", None)
        if uc_ref is not None:
            env["UC_REF"] = uc_ref
        return subprocess.run(
            ["bash", "-c", script], env=env, capture_output=True, text=True
        )

    def assert_checkout(self, result, sha):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=self.checkout), sha)

    def test_pin_on_main(self):
        self.assert_checkout(self.run_checkout(self.main, "main"), self.main)

    def test_unmerged_pull_request_pin(self):
        pr_head = self.git(
            "commit-tree",
            f"{self.pin}^{{tree}}",
            "-p",
            self.pin,
            input="Updated pull request\n",
        )
        self.git("update-ref", "refs/pull/2093/head", pr_head)
        self.assert_checkout(
            self.run_checkout(self.pin, "refs/pull/2093/head"), self.pin
        )

    def test_reject_pin_outside_upstream_ref(self):
        result = self.run_checkout(self.pin, "main")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            f"UC_PIN_SHA={self.pin} is not reachable from main", result.stderr
        )

    def test_floating_main(self):
        result = self.run_checkout(self.pin, "refs/pull/2093/head", uc_ref="main")
        self.assert_checkout(result, self.main)

    def test_explicit_release_ref(self):
        result = self.run_checkout(self.main, "main", uc_ref="refs/tags/test-release")
        self.assert_checkout(result, self.pin)

    def test_reject_base_version_mismatch(self):
        result = self.run_checkout(
            self.pin, "refs/pull/2093/head", base_version="0.9.0-SNAPSHOT"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("UC_BASE_VERSION='0.9.0-SNAPSHOT'", result.stderr)


if __name__ == "__main__":
    unittest.main()
