#!/usr/bin/env python

from setuptools import setup, find_packages


version = '0.4.2'

setup(
    name='dino-service',
    version=version,
    description="Distributed Notifications",
    long_description="""Distributed messaging server""",
    classifiers=[],
    keywords='messaging,chat,distributed',
    author='Oscar Eriksson',
    author_email='oscar.eriks@gmail.com',
    url='https://github.com/thenetcircle/dino-service',
    license='LICENSE',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'arrow',
        'bcrypt',
        'black',
        'cassandra-driver',
        'fakeredis',
        'fastapi',
        'gitdb',
        'gmqtt',
        'gnenv',
        'kafka-python',
        'lz4',
        'psycopg2-binary',
        'redis',
        'PyYAML',
        'raven',
        'requests',
        'sortedcontainers',
        'sqlalchemy',
        'strict-rfc3339',
        'urllib3',
        'uvicorn',
        'uvloop',
        'websockets',
        'activitystreams',
    ])
