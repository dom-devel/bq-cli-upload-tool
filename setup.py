# -*- coding: utf-8 -*-


"""setup.py: setuptools control."""


import re
from setuptools import setup


version = re.search(
    '^__version__\s*=\s*"(.*)"',
    open('bq_upload/bootstrap.py').read(),
    re.M
    ).group(1)


with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")


setup(
    name="bq-upload",
    packages=["bq_upload"],
    entry_points={
        "console_scripts": ['bq-upload = bq_upload.bootstrap:main']
        },
    version=version,
    description="A python command line package, which makes it easy to upload files to BigQuery.",
    long_description=long_descr,
    author="Dominic Woodman",
    author_email="domwoodman@gmail.com",
    url="https://www.domwoodman.com",
    include_package_data=True
    )