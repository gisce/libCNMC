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
      version='0.6.0',
      license='General Public Licence 2',
      long_description='''Long description''',
      provides=['libcnmc'],
      install_requires=INSTALL_REQUIRES,
      packages=find_packages(exclude=['tests']),
      package_data=PACKAGES_DATA,
      entry_points={
          'console_scripts': [
              'f1 = libcnmc.cli:cir_4_2014_f1',
              'f1bis = libcnmc.cli:cir_4_2014_f1bis',
              'f11 = libcnmc.cli:cir_4_2014_f11',
              'cnmc = libcnmc.cli:invoke'
              '4603_lat = libcnmc.cli:res_4603_lat',
              '4603_lbt = libcnmc.cli:res_4603_lbt',
              '4603_sub = libcnmc.cli:res_4603_sub',
              '4603_pos = libcnmc.cli:res_4603_pos',
              '4603_maq = libcnmc.cli:res_4603_maq',
              '4603_des = libcnmc.cli:res_4603_des',
              '4603_fia = libcnmc.cli:res_4603_fia',
              '4603_cts = libcnmc.cli:res_4603_cts',
              '4603_inv = libcnmc.cli:res_4603_inv',
              '4603_cinimaq = libcnmc.cli:res_4603_cinimaq',
              '4603_cinipos = libcnmc.cli:res_4603_cinipos',
              'update_cnmc_stats = libcnmc.cli:update_cnmc_stats',
              'update_cinis_comptador = libcnmc.cli:update_cinis_comptador'
          ]
      })

