#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mongodb',
      version='2.1.2',
      description='Singer.io tap for extracting data from MongoDB',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mongodb'],
      install_requires=[
          'singer-python==5.8.0',
          'pymongo==3.12.3',
          'tzlocal==2.0.0',
          'terminaltables==3.1.0',
      ],
      extras_require={
          'dev': [
              'pylint',
              'nose',
              'ipdb'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mongodb=tap_mongodb:main
      ''',
      packages=['tap_mongodb', 'tap_mongodb.sync_strategies'],

)
