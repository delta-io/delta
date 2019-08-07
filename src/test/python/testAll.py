#
# Copyright 2019 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
from os import listdir
from os.path import isfile, join

from test_deltatable import DeltaTableTests


def testAll():
    # Check env conf first, the same as circleci conf.
    # for now we assume local machine has all of the requirements to run the script.

    # And then, run all of the test under test/python directory, each of them has
    # main entry point to execute, which is python's unittest testing framework.
    current_path = os.path.abspath(os.path.dirname(__file__))
    pyfiles = [f for f in listdir(current_path)
               if isfile(join(current_path, f)) and f.endswith(".py") and f != "__init__.py"]
    for pyfile in pyfiles:
        pass


if __name__ == "__main__":
    testAll()
