#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup pel la llibreria de Exportació dels 1048.
"""
import sys
from setuptools import setup, find_packages


PACKAGES_DATA = {}


INSTALL_REQUIRES = ['progressbar', 'click']
if sys.version_info[1] < 6:
    INSTALL_REQUIRES += ['multiprocessing']

setup(name='libcnmc',
      description='Generació fitxers CNMC',
      author='GISCE-TI, S.L.',
      author_email='devel@gisce.net',
      url='http://www.gisce.net',
      version='0.1.0',
      license='General Public Licence 2',
      long_description='''Long description''',
      provides=['libcnmc'],
      install_requires=INSTALL_REQUIRES,
      packages=find_packages(),
      package_data=PACKAGES_DATA,
      entry_points={
          'console_scripts': ['f1 = libcnmc.cir_4_2014.F1:main']
      })

