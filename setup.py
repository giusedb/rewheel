#!/usr/bin/env python

from setuptools import setup

setup(name='rewheel',
      version='0.1',
      description='Rewheel Flask modules',
      author='Giuseppe Di Bona',
      author_email='giusedb@gmail.com',
      url='http://www.demalogic.it',
      packages=['rewheel'],
      classifiers=[
          "Development Status :: 2 - Pre-Alpha",
          "Topic :: Web framework",
          "License :: OSI Approved :: BSD License",
      ],
      keywords="web realtime realtime-web full-realtime rest restful",
      install_requires=[
          'Flask',
          'pyDAL',
          'redis',
          'ujson',
          'sockjs-tornado',
          'flask-cors',
          'logging-tree',
          'pyyaml',
      ]
     )
