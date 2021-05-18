# !/usr/bin/env python
#
#  Copyright (2020) The Delta Lake Project Authors.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import os
import sys
import subprocess
import argparse


def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", default=False, action='store_true')
    args = parser.parse_args()
    global verbose
    verbose = args.verbose

    # Set up the directories
    docs_root_dir = os.path.dirname(os.path.realpath(__file__))
    repo_root_dir = os.path.dirname(docs_root_dir)
    scaladoc_gen_dir = repo_root_dir + "/core/target/scala-2.12/unidoc"
    javadoc_gen_dir = repo_root_dir + "/core/target/javaunidoc"
    all_api_docs_final_dir = docs_root_dir + "/_site/api"
    scala_api_docs_final_dir = all_api_docs_final_dir + "/scala"
    java_api_docs_final_dir = all_api_docs_final_dir + "/java"
    sphinx_gen_dir = repo_root_dir + "/docs/python"
    sphinx_cp_dir = sphinx_gen_dir + "/_build/html"
    sphinx_docs_final_dir = all_api_docs_final_dir + "/python"

    # Generate Java and Scala docs
    print("## Generating ScalaDoc and JavaDoc ...")
    with WorkingDirectory(repo_root_dir):
        run_cmd(["build/sbt", ";clean;unidoc"], stream_output=verbose)

    # Update Scala docs
    print("## Patching ScalaDoc ...")
    with WorkingDirectory(scaladoc_gen_dir):
        # Patch the js and css files
        append(docs_root_dir + "/api-docs.js", "./lib/template.js")  # append new js functions
        append(docs_root_dir + "/api-docs.css", "./lib/template.css")  # append new styles

    # Update Java docs
    print("## Patching JavaDoc ...")
    with WorkingDirectory(javadoc_gen_dir):
        # Find html files to patch
        (_, stdout, _) = run_cmd(["find", ".", "-name", "*.html", "-mindepth", "2"])
        log("HTML files found:\n" + stdout)
        javadoc_files = [line for line in stdout.split('\n') if line.strip() != '']

        js_script_start = '<script defer="defer" type="text/javascript" src="'
        js_script_end = '"></script>'

        # Patch the html files
        for javadoc_file in javadoc_files:
            # Generate relative path to js files based on how deep the html file is
            slash_count = javadoc_file.count("/")
            i = 1
            path_to_js_file = ""
            while i < slash_count:
                path_to_js_file = path_to_js_file + "../"
                i += 1

            # Create script elements to load new js files
            javadoc_jquery_script = \
                js_script_start + path_to_js_file + "lib/jquery.min.js" + js_script_end
            javadoc_api_docs_script = \
                js_script_start + path_to_js_file + "lib/api-javadocs.js" + js_script_end
            javadoc_script_elements = javadoc_jquery_script + javadoc_api_docs_script

            # Add script elements to body of the html file
            replace(javadoc_file, "</body>", javadoc_script_elements + "</body>")

        # Patch the js and css files
        run_cmd(["mkdir", "-p", "./lib"])
        run_cmd(["cp", scaladoc_gen_dir + "/lib/jquery.min.js", "./lib/"])  # copy from ScalaDocs
        run_cmd(["cp", docs_root_dir + "/api-javadocs.js", "./lib/"])   # copy new js file
        append(docs_root_dir + "/api-javadocs.css", "./stylesheet.css")  # append new styles

    # Generate Python docs
    print('## Generating Python(Sphinx) docs ...')
    with WorkingDirectory(sphinx_gen_dir):
        run_cmd(["make", "html"], stream_output=verbose)

    # Copy to final location
    log("Copying to API doc directory %s" % all_api_docs_final_dir)
    run_cmd(["rm", "-rf", all_api_docs_final_dir])
    run_cmd(["mkdir", "-p", all_api_docs_final_dir])
    run_cmd(["cp", "-r", scaladoc_gen_dir, scala_api_docs_final_dir])
    run_cmd(["cp", "-r", javadoc_gen_dir, java_api_docs_final_dir])
    run_cmd(["cp", "-r", sphinx_cp_dir, sphinx_docs_final_dir])

    print("## API docs generated in " + all_api_docs_final_dir)


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
    log("Running command %s" % str(cmd))
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
        if sys.version_info >= (3, 0):
            stdout = stdout.decode("UTF-8")
            stderr = stderr.decode("UTF-8")

        exit_code = child.wait()
        if throw_on_error and exit_code is not 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


def append(src, dst):
    log("Appending %s to %s" % (src, dst))
    fin = open(src, "r")
    str = fin.read()
    fin.close()
    fout = open(dst, "a")
    fout.write(str)
    fout.close()


def replace(file, pattern, replacement):
    log("Replacing %s with %s in file %s" % (pattern, replacement, file))
    fin = open(file, "r")
    str = fin.read()
    fin.close()
    str = str.replace(pattern, replacement)
    fout = open(file, "w")
    fout.write(str)
    fout.close()


# pylint: disable=too-few-public-methods
class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)


def log(str):
    if verbose:
        print(str)


verbose = False

if __name__ == "__main__":
    # pylint: disable=e1120
    main()
