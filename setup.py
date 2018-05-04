#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup pel la llibreria de Exportació dels 1048.
"""
import sys
from setuptools import setup, find_packages


PACKAGES_DATA = {}


INSTALL_REQUIRES = [
    'progressbar', 'click', 'libcomxml',
    'chardet', 'pyproj', 'osconf', 'cerberus>=1.0'
]
DEPENDENCY_LINKS = [
    'https://github.com/gisce/ooop/archive/xmlrpc_transaction.zip'
]
if sys.version_info[1] < 6:
    INSTALL_REQUIRES += ['multiprocessing']

setup(name='libcnmc',
      description='Generació fitxers CNMC',
      author='GISCE-TI, S.L.',
      author_email='devel@gisce.net',
      url='http://www.gisce.net',
      version='0.18.44',
      license='General Public Licence 2',
      long_description='''Long description''',
      provides=['libcnmc'],
      install_requires=INSTALL_REQUIRES,
      packages=find_packages(exclude=['tests']),
      dependency_links=DEPENDENCY_LINKS,
      package_data=PACKAGES_DATA,
      entry_points={
          'console_scripts': [
              'f1 = libcnmc.cli:cir_4_2014_f1',
              'f1bis = libcnmc.cli:cir_4_2014_f1bis',
              'f11 = libcnmc.cli:cir_4_2014_f11',
              'cnmc = libcnmc.cli:cli',
              'cnmc_checker = libcnmc.checker:node_check'
          ]
      })

