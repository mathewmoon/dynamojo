#!/usr/bin/env python3
from setuptools import setup

description = "WIP"
with open("README.md", "r") as f:
  long_description = f.read()

setup(
    name="Dynamojo",
    version="0.0.1",
    description=description,
    long_description=long_description,
    install_requires = [
      "requests"
    ],
    long_description_content_type="text/markdown",
    author="Mathew Moon",
    author_email="me@mathewmoon.net",
    python_requires=">=3.8",
    packages=["dynamojo"]
)
