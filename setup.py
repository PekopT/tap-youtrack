#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-youtrack",
    version="0.33",
    description="Singer.io Youtrack tap",
    author="Farhrenheit",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_youtrack"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
        "decouple",
        "retry",
        "pytz"
    ],
    entry_points="""
    [console_scripts]
    tap-youtrack=tap_youtrack:main
    """,
    packages=["tap_youtrack"],
    package_data = {
        "schemas": ["tap_youtrack/schemas/*.json"]
    },
    include_package_data=False,
)
