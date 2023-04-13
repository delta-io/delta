#
# Copyright (2021) The Delta Lake Project Authors.
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

FROM python:3.7.3-stretch

RUN apt-get update && apt-get -y install openjdk-8-jdk

RUN pip install pyspark==3.3.2

RUN pip install mypy==0.910

RUN pip install importlib_metadata==3.10.0

RUN pip install cryptography==37.0.4

# We must install cryptography before twine. Else, twine will pull a newer version of
# cryptography that requires a newer version of Rust and may break tests.
RUN pip install twine==4.0.1

RUN pip install wheel==0.33.4

RUN pip install setuptools==41.0.1

# Do not add any non-deterministic changes (e.g., copy from files 
# from repo) in this Dockerfile, so that the  docker image 
# generated from this can be reused across builds.
