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

# Debian buster LTS is until June 30th 2024. [TODO] Upgrade to a newer version before then.
FROM openjdk:8-jdk-buster

# Install pip3
RUN apt-get update && apt-get install -y python3-pip
# Upgrade pip. This is needed to use prebuilt wheels for packages cffi (dep of cryptography) and
# cryptography. Otherwise, building wheels for these packages fails.
RUN pip3 install --upgrade pip

RUN pip3 install pyspark==3.4.0

RUN pip3 install mypy==0.910

RUN pip3 install importlib_metadata==3.10.0

RUN pip3 install cryptography==37.0.4

# We must install cryptography before twine. Else, twine will pull a newer version of
# cryptography that requires a newer version of Rust and may break tests.
RUN pip3 install twine==4.0.1

RUN pip3 install wheel==0.33.4

RUN pip3 install setuptools==41.0.1

# Do not add any non-deterministic changes (e.g., copy from files 
# from repo) in this Dockerfile, so that the  docker image 
# generated from this can be reused across builds.
