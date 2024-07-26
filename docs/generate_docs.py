#!/usr/bin/env python3

import argparse
import os
import subprocess
import random
import shutil
import string
import tempfile


def main():
    """Script to manage the deployment of Delta Lake docs to the hosting bucket.
       To build the docs:
       $ generate_docs --livehtml

    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--livehtml",
        action="store_true",
        help="Build and serve a local build of docs")
    parser.add_argument(
        "--api-docs",
        action="store_true",
        help="Generate the API docs")

    args = parser.parse_args()
    
    # Assert that env var _DELTA_LAKE_RELEASE_VERSION_ (used by conf.py) is set
    try:
        os.environ["_DELTA_LAKE_RELEASE_VERSION_"]
    except KeyError:
        raise KeyError(f"Environment variable _DELTA_LAKE_RELEASE_VERSION_ not set.")

    docs_root_dir = os.path.dirname(os.path.realpath(__file__))
    api_docs_root_dir = os.path.join(docs_root_dir, "apis")

    with WorkingDirectory(docs_root_dir):
        html_output = os.path.join(docs_root_dir, '_site', 'html')
        if os.path.exists(html_output):
           print("Deleting previous output directory %s" % (html_output))
           shutil.rmtree(html_output)

        os.makedirs(html_output, exist_ok=False)

        html_source = os.path.join(docs_root_dir, 'source')
        print("Building content")
        env = { "TARGET_CLOUD": "delta-oss-only" }

        sphinx_cmd = "sphinx-build"
        if args.livehtml:
            sphinx_cmd = "sphinx-autobuild"
        build_docs_args = "%s -b html -d /tmp/build/doctrees %s %s" % (
            sphinx_cmd, html_source, html_output)
        if args.api_docs:
            generate_and_copy_api_docs(api_docs_root_dir, html_output)
        run_cmd(build_docs_args, env=env, shell=True, stream_output=True)


def generate_and_copy_api_docs(api_docs_root_dir, target_loc):
    return
    print("Building API docs")

    with WorkingDirectory(target_loc):
        script_path = os.path.join(api_docs_root_dir, "generate_api_docs.py")
        api_docs_dir = os.path.join(api_docs_root_dir,  "_site", "api")
        run_cmd(["python3", script_path], stream_output=True)
        assert os.path.exists(api_docs_dir), \
            "Doc generation didn't create the expected api directory"
        api_docs_dest_dir = os.path.join(target_loc, "api")
        shutil.copytree(api_docs_dir, api_docs_dest_dir)


class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, type, value, traceback):
        os.chdir(self.old_workdir)


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    """Runs a command as a child process.

    A convenience wrapper for running a command from a Python script.
    Keyword arguments:
    cmd -- the command to run, as a list of strings
    throw_on_error -- if true, raises an Exception if the exit code of the program is nonzero
    env -- additional environment variables to be defined when running the child process
    stream_output -- if true, does not capture standard output and error; if false, captures these
      streams and returns them

    Note on the return value: If stream_output is true, then only the exit code is returned. If
    stream_output is false, then a tuple of the exit code, standard output and standard error is
    returned.
    """
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % exit_code)
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return exit_code, stdout.decode("utf-8"), stderr.decode("utf-8")


if __name__ == "__main__":
    main()
