#!/usr/bin/env python

from setuptools import setup

setup(
    name="psycho",
    version="0.0.6",
    description="An ultra simple wrapper for Python psycopg2 with very basic functionality",
    author="Scott Clark",
    author_email="scott@usealloy.io",
    packages=['psycho'],
    download_url="http://github.com/usealloy/psycho",
    license="MIT",
    classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Programming Language :: Python",
            "Natural Language :: English",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Topic :: Database",
            "Topic :: Software Development :: Libraries"
    ],
    install_requires=["psycopg2"]
)
