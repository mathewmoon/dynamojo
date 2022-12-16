#!/usr/bin/env python3
from setuptools import setup

description = "Object Modeling for Dynamodb"

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="Dynamojo",
    version="0.1.0",
    description=description,
    long_description=long_description,
    install_requires=["pydantic", "boto3"],
    long_description_content_type="text/markdown",
    author="Mathew Moon",
    author_email="me@mathewmoon.net",
    url="https://github.com/mathewmoon/dynamojo",
    python_requires=">=3.8",
    packages=["dynamojo"],
)
