#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys

from setuptools import setup
from setuptools.command.install import install


# delta.io version
def get_version_from_sbt():
    with open("version.sbt") as fp:
        version = fp.read().strip()
    return version.split('"')[1]


VERSION = get_version_from_sbt()
MAJOR_VERSION = int(VERSION.split(".")[0])

if MAJOR_VERSION < 4:
    packages_arg = ['delta']
    install_requires_arg = ['pyspark>=3.5.2,<3.6.0', 'importlib_metadata>=1.0.0']
    python_requires_arg = '>=3.6'
else:  # MAJOR_VERSION >= 4
    # Delta 4.0+ contains Delta Connect code and uses Spark 4.0+
    packages_arg = ['delta', 'delta.connect', 'delta.connect.proto']
    install_requires_arg = ['pyspark>=4.0.0', 'importlib_metadata>=1.0.0']
    python_requires_arg = '>=3.9'

class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != VERSION:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, VERSION
            )
            sys.exit(info)


with open("python/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="delta-spark",
    version=VERSION,
    description="Python APIs for using Delta Lake with Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/delta-io/delta/",
    project_urls={
        'Source': 'https://github.com/delta-io/delta',
        'Documentation': 'https://docs.delta.io/latest/index.html',
        'Issues': 'https://github.com/delta-io/delta/issues'
    },
    author="The Delta Lake Project Authors",
    author_email="delta-users@googlegroups.com",
    license="Apache-2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Typing :: Typed",
    ],
    keywords='delta.io',
    package_dir={'': 'python'},
    packages=packages_arg,
    package_data={
        'delta': ['py.typed'],
    },
    install_requires=install_requires_arg,
    python_requires=python_requires_arg,
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
