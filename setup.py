#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'aioredis',
]

test_requirements = [
    'pytest',
    'pytz',
]

setup(
    name='aioredis_timeseries',
    version='0.0.1',
    description="Timeseries API built on top of Redis",
    long_description=readme + '\n\n' + history,
    author="Ryan Anguiano",
    author_email='ryan.anguiano@gmail.com',
    url='https://github.com/ryananguiano/aioredis-timeseries',
    py_modules=['aioredis_timeseries'],
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='aioredis_timeseries',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
