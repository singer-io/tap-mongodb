#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mongodb',
      version='1.9.21',
      description='Singer.io tap for extracting data from MongoDB',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mongodb'],
      install_requires=[
          'attrs==16.3.0',
          'pendulum==1.2.0',
          'singer-python==5.3.3',
          'backoff==1.3.2',
          'pymongo==3.7.1'
      ],
      entry_points='''
          [console_scripts]
          tap-mongodb=tap_mongodb:main
      ''',
      packages=['tap_mongodb', 'tap_mongodb.sync_strategies'],

)
