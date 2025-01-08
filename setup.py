#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mongodb',
      version='3.1.4',
      description='Singer.io tap for extracting data from MongoDB',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mongodb'],
      install_requires=[
          'singer-python==6.0.0',
          'pymongo==4.10.1',
          'tzlocal==2.0.0',
      ],
      extras_require={
          'dev': [
              'pylint',
              'nose2',
              'ipdb'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mongodb=tap_mongodb:main
      ''',
      packages=['tap_mongodb', 'tap_mongodb.sync_strategies'],

)
