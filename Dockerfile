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
FROM ubuntu:focal-20221019

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get install -y curl
RUN apt-get install -y wget
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y python3.8
RUN apt-get install -y python3-pip
RUN apt-get install -y git

# Upgrade pip. This is needed to use prebuilt wheels for packages cffi (dep of cryptography) and
# cryptography. Otherwise, building wheels for these packages fails.
RUN pip3 install --upgrade pip

# Update the pip version to 24.0. By default `pyenv.run` installs the latest pip version
# available. From version 24.1, `pip` doesn't allow installing python packages
# with version string containing `-`. In Delta-Spark case, the pypi package generated has
# `-SNAPSHOT` in version (e.g. `3.3.0-SNAPSHOT`) as the version is picked up from
# the`version.sbt` file.
RUN pip install pip==24.0 setuptools==69.5.1 wheel==0.43.0

RUN pip3 install pyspark==3.5.3

RUN pip3 install mypy==0.982

RUN pip3 install pydocstyle==3.0.0

RUN pip3 install pandas==1.0.5

RUN pip3 install pyarrow==8.0.0

RUN pip3 install numpy==1.20.3

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
