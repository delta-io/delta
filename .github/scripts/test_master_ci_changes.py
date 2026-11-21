import importlib.util
import os
import pathlib
import subprocess
import tempfile
import unittest
from urllib.error import HTTPError
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("master_ci_changes.py")
SPEC = importlib.util.spec_from_file_location("master_ci_changes", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class MasterCIChangesTest(unittest.TestCase):
    def test_flink_only_diff(self):
        result = MODULE.selections(["flink/src/main/Flink.java"])
        self.assertTrue(result["flink_test"])
        self.assertTrue(result["unidoc"])
        self.assertFalse(result["kernel_unitycatalog_test"])
        self.assertTrue(result["spark_test"])

    def test_markdown_only_diff(self):
        result = MODULE.selections(["README.md"])
        self.assertEqual(
            {name for name, selected in result.items() if selected}, {"unidoc"}
        )

    def test_no_prior_run_runs_everything(self):
        text = self._run_main_with_lookup(None)
        self.assertTrue(all(f"{name}=true" in text for name in MODULE.WORKFLOWS))

    def test_lookup_error_runs_everything(self):
        text = self._run_main_with_lookup(side_effect=RuntimeError("API failed"))
        self.assertTrue(all(f"{name}=true" in text for name in MODULE.WORKFLOWS))

    def test_selector_test_failure_runs_everything(self):
        with tempfile.NamedTemporaryFile() as output, mock.patch.dict(
            os.environ,
            {
                "MASTER_CI_SELECTOR_TESTS_PASSED": "false",
                "GITHUB_OUTPUT": output.name,
            },
            clear=True,
        ):
            MODULE.main()
            output.seek(0)
            text = output.read().decode()
        self.assertTrue(all(f"{name}=true" in text for name in MODULE.WORKFLOWS))

    def test_diff_spans_skipped_commits(self):
        with self._repository() as repository:
            base = self._commit(repository, "base.txt", "base", "base")
            self._commit(repository, "flink/change.scala", "change", "skipped")
            head = self._commit(repository, "README.md", "head", "head")
            paths = self._changed_paths(repository, base, head)
        self.assertIn("flink/change.scala", paths)
        self.assertTrue(MODULE.selections(paths)["flink_test"])

    def test_rename_out_of_kernel_selects_kernel_workflows(self):
        paths = self._rename_paths("kernel/src/Foo.scala", "other/Foo.scala")
        self.assertIn("kernel/src/Foo.scala", paths)
        self.assertIn("other/Foo.scala", paths)
        selected = MODULE.selections(paths)
        self.assertTrue(selected["kernel_unitycatalog_test"])
        self.assertTrue(selected["flink_test"])

    def test_rename_out_of_flink_selects_flink(self):
        paths = self._rename_paths("flink/src/Foo.scala", "other/Foo.scala")
        self.assertIn("flink/src/Foo.scala", paths)
        self.assertIn("other/Foo.scala", paths)
        self.assertTrue(MODULE.selections(paths)["flink_test"])

    def test_rename_code_to_markdown_is_not_docs_only(self):
        paths = self._rename_paths("src/Foo.scala", "docs/Foo.md")
        self.assertIn("src/Foo.scala", paths)
        self.assertIn("docs/Foo.md", paths)
        selected = MODULE.selections(paths)
        self.assertTrue(selected["build"])
        self.assertTrue(selected["spark_test"])

    def test_rerun_skips_current_successful_run(self):
        pages = [
            ([{"name": "Master CI", "head_sha": "current"}], '<next>; rel="next"'),
            ([{"name": "Master CI", "head_sha": "older"}], None),
        ]
        with mock.patch.object(MODULE, "_request_runs", side_effect=pages) as request:
            result = MODULE.latest_successful_head("tdas/delta", "master", "token", "current")
        self.assertEqual(result, "older")
        self.assertEqual(request.call_count, 2)

    def test_404_fallback_name_scan_paginates(self):
        error = HTTPError("url", 404, "not found", {}, None)
        pages = [
            error,
            ([{"name": "Other", "head_sha": "other"}], '<next>; rel="next"'),
            ([{"name": "Master CI", "head_sha": "older"}], None),
        ]
        with mock.patch.object(MODULE, "_request_runs", side_effect=pages) as request:
            result = MODULE.latest_successful_head("tdas/delta", "master", "token", "current")
        self.assertEqual(result, "older")
        self.assertEqual(request.call_count, 3)

    def _run_main_with_lookup(self, value=None, side_effect=None):
        with tempfile.NamedTemporaryFile() as output, mock.patch.dict(
            os.environ,
            {
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_REPOSITORY": "tdas/delta",
                "GITHUB_REF_NAME": "branch-mci-test",
                "GITHUB_TOKEN": "token",
                "GITHUB_SHA": "head",
                "GITHUB_OUTPUT": output.name,
            },
            clear=True,
        ), mock.patch.object(
            MODULE, "latest_successful_head", return_value=value, side_effect=side_effect
        ):
            MODULE.main()
            output.seek(0)
            return output.read().decode()

    def _rename_paths(self, old_path, new_path):
        with self._repository() as repository:
            base = self._commit(repository, old_path, "content", "base")
            old = pathlib.Path(repository, old_path)
            new = pathlib.Path(repository, new_path)
            new.parent.mkdir(parents=True, exist_ok=True)
            old.rename(new)
            self._git(repository, "add", "-A")
            self._git(repository, "commit", "-qm", "rename")
            head = self._git(repository, "rev-parse", "HEAD").stdout.strip()
            return self._changed_paths(repository, base, head)

    def _repository(self):
        class Repository:
            def __enter__(inner_self):
                inner_self.directory = tempfile.TemporaryDirectory()
                directory = inner_self.directory.name
                subprocess.run(["git", "init", "-q", directory], check=True)
                self._git(directory, "config", "user.email", "test@example.com")
                self._git(directory, "config", "user.name", "Test")
                self._git(directory, "config", "core.hooksPath", "/dev/null")
                return directory

            def __exit__(inner_self, *args):
                inner_self.directory.cleanup()

        return Repository()

    def _commit(self, repository, relative_path, contents, message):
        path = pathlib.Path(repository, relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        self._git(repository, "add", ".")
        self._git(repository, "commit", "-qm", message)
        return self._git(repository, "rev-parse", "HEAD").stdout.strip()

    def _changed_paths(self, repository, base, head):
        previous = os.getcwd()
        try:
            os.chdir(repository)
            return MODULE.changed_paths(base, head)
        finally:
            os.chdir(previous)

    @staticmethod
    def _git(repository, *args):
        return subprocess.run(
            ["git", *args],
            cwd=repository,
            check=True,
            text=True,
            capture_output=True,
        )


if __name__ == "__main__":
    unittest.main()
