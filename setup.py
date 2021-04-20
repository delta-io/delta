#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys

from setuptools import setup
from setuptools.command.install import install

# delta.io version
VERSION = "0.5.0"

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

setup(
    name="delta.io",
    version=VERSION,
    description="Python wrapper for Delta Lake",
    url="https://github.com/delta-io/delta",
    author="TODO",
    author_email="TODO",
    license="Apache-2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
    ],
    keywords='delta.io',
    package_dir = {'': 'python'},
    packages=['delta'],
    install_requires=[
        'pyspark>=2.4.2',
    ],
    python_requires='>=3',
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
