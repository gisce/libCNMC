#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Inventari
"""
from datetime import datetime
import csv
import os
import traceback
from chardet import detect

from libcnmc.core import cnmc_inventari

try:
    from raven import Client
except:
    Client = None

QUIET = False
from libcnmc import VERSION


class INV():
    def __init__(self, **kwargs):
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Inventari'
        self.report_name = 'CNMC INVENTARI XML'
        self.liniesat = kwargs.pop('liniesat')
        self.liniesbt = kwargs.pop('liniesbt')
        self.subestacions = kwargs.pop('subestacions')
        self.posicions = kwargs.pop('posicions')
        self.maquinas = kwargs.pop('maquinas')
        self.despatxos = kwargs.pop('despatxos')
        self.fiabilidad = kwargs.pop('fiabilidad')
        self.transformacion = kwargs.pop('transformacion')
        self.file_out = kwargs.pop('output')
        self.pla_inversions_xml = cnmc_inventari.Empresa(self.codi_r1)
        if 'SENTRY_DSN' in os.environ and Client:
            self.raven = Client()
            self.raven.tags_context({'version': VERSION})
        else:
            self.raven = None

    def check_encoding(self):
        input_files = ['liniesat', 'liniesbt', 'subestacions', 'posicions',
                       'maquinas', 'despatxos', 'fiabilidad', 'transformacion']
        for input_f in input_files:
            with open(getattr(self, input_f), 'r') as f:
                result = detect(f.read())
                if result['encoding'] not in ('utf-8', 'ascii'):
                    raise Exception('File: %s is not in UTF-8.' % input_f)

    def open_csv_file(self, csv_file):
        reader = csv.reader(open(csv_file), delimiter=';')
        return reader

    def tractar_linies(self, arxiucsvlinies):
        reader = self.open_csv_file(arxiucsvlinies)
        for row in reader:
            if not row:
                continue
            linia = cnmc_inventari.Linea()

            identificador = row[0]
            cini = row[1]
            origen = row[2]
            destino = row[3]
            codigo_tipo_linea = row[4]
            codigo_ccaa_1 = row[5]
            codigo_ccaa_2 = row[6]
            participacion = row[7]
            fecha_aps = row[8]
            fecha_baja = row[9]
            numero_circuitos = row[10]
            numero_conductores = row[11]
            longitud = row[12]
            seccion = row[13]
            capacidad = row[14]

            linia.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'origen': '%s' % origen or ' ',
                'destino': '%s' % destino or ' ',
                'codigo_tipo_linea': '%s' % codigo_tipo_linea,
                'codigo_ccaa_1': '%s' % codigo_ccaa_1,
                'codigo_ccaa_2': '%s' % codigo_ccaa_2,
                'participacion': '%s' % participacion,
                'fecha_aps': '%s' % fecha_aps or ' ',
                'fecha_baja': '%s' % fecha_baja or ' ',
                'numero_circuitos': '%s' % numero_circuitos,
                'numero_conductores': '%s' % numero_conductores,
                'longitud': '%s' % longitud,
                'seccion': '%s' % seccion,
                'capacidad': '%s' % capacidad
            })
            self.pla_inversions_xml.linea.append(linia)

    def tractar_linies_at(self):
        self.tractar_linies(self.liniesat)

    def tractar_linies_bt(self):
        self.tractar_linies(self.liniesbt)

    def tractar_sub(self, arxiucsvsub):
        reader = self.open_csv_file(arxiucsvsub)
        for row in reader:
            if not row:
                continue
            sub = cnmc_inventari.Subestacion()

            identificador = row[0]
            cini = row[1]
            denominacion = row[2]
            codigo_tipo_posicion = row[3]
            codigo_ccaa = row[4]
            participacion = row[5]
            fecha_aps = row[6]
            fecha_baja = row[7]
            posiciones = row[8]

            sub.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'denominacion': '%s' % denominacion or ' ',
                'codigo_tipo_posicion': '%s' % codigo_tipo_posicion,
                'codigo_ccaa': '%s' % codigo_ccaa,
                'participacion': '%s' % participacion,
                'fecha_aps': '%s' % fecha_aps or ' ',
                'fecha_baja': '%s' % fecha_baja or ' ',
                'posiciones': '%s' % posiciones,
            })
            self.pla_inversions_xml.linea.append(sub)

    def tractar_subestacions(self):
        self.tractar_sub(self.subestacions)

    def tractar_pos(self, arxiucsvpos):
        reader = self.open_csv_file(arxiucsvpos)
        for row in reader:
            if not row:
                continue
            pos = cnmc_inventari.Posicion()

            identificador = row[0]
            cini = row[1]
            denominacion = row[2]
            codigo_tipo_posicion = row[3]
            codigo_ccaa = row[4]
            participacion = row[5]
            fecha_aps = row[6]
            fecha_baja = row[7]

            pos.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'denominacion': '%s' % denominacion or ' ',
                'codigo_tipo_posicion': '%s' % codigo_tipo_posicion,
                'codigo_ccaa': '%s' % codigo_ccaa,
                'participacion': '%s' % participacion,
                'fecha_aps': '%s' % fecha_aps or ' ',
                'fecha_baja': '%s' % fecha_baja or ' ',
            })
            self.pla_inversions_xml.linea.append(pos)

    def tractar_posicions(self):
        self.tractar_pos(self.posicions)

    def tractar_maq(self, arxiucsvmaq):
        reader = self.open_csv_file(arxiucsvmaq)
        for row in reader:
            if not row:
                continue
            maq = cnmc_inventari.Maquina()

            identificador = row[0]
            cini = row[1]
            denominacion = row[2]
            codigo_tipo_maquina = row[3]
            codigo_zona = row[4]
            codigo_ccaa = row[5]
            participacion = row[6]
            fecha_aps = row[7]
            fecha_baja = row[8]
            capacidad = row[9]

            maq.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'denominacion': '%s' % denominacion or ' ',
                'codigo_tipo_posicion': '%s' % codigo_tipo_maquina,
                'codigo_zona': '%s' % codigo_zona,
                'codigo_ccaa': '%s' % codigo_ccaa,
                'participacion': '%s' % participacion,
                'fecha_aps': '%s' % fecha_aps or ' ',
                'fecha_baja': '%s' % fecha_baja or ' ',
                'capacidad': '%s' % capacidad,
            })
            self.pla_inversions_xml.linea.append(maq)

    def tractar_maquines(self):
        self.tractar_maq(self.maquinas)

    def tractar_desp(self, arxiucsvdesp):
        reader = self.open_csv_file(arxiucsvdesp)
        for row in reader:
            if not row:
                continue
            despt = cnmc_inventari.Despacho()

            identificador = row[0]
            cini = row[1]
            denominacion = row[2]
            anyo_ps = row[3]
            vai = row[4]

            despt.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'denominacion': '%s' % denominacion or ' ',
                'anyo_ps': '%s' % anyo_ps,
                'vai': '%s' % vai,
            })
            self.pla_inversions_xml.linea.append(despt)

    def tractar_despatxos(self):
        self.tractar_desp(self.despatxos)

    def tractar_fia(self, arxiucsvfia):
        reader = self.open_csv_file(arxiucsvfia)
        for row in reader:
            if not row:
                continue
            despt = cnmc_inventari.Fiabilidad()

            identificador = row[0]
            cini = row[1]
            denominacion = row[2]
            codigo_tipo_inst = row[3]
            codigo_ccaa = row[4]
            fecha_aps = row[5]
            fecha_baja = row[6]

            despt.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'denominacion': '%s' % denominacion or ' ',
                'codigo_tipo_inst': '%s' % codigo_tipo_inst or ' ',
                'codigo_ccaa': '%s' % codigo_ccaa,
                'fecha_aps': '%s' % fecha_aps or ' ',
                'fecha_baja': '%s' % fecha_baja or ' ',
            })
            self.pla_inversions_xml.linea.append(despt)

    def tractar_equips_fiabilitat(self):
        self.tractar_fia(self.fiabilidad)

    def tractar_trans(self, arxiucsvtrans):
        reader = self.open_csv_file(arxiucsvtrans)
        for row in reader:
            if not row:
                continue
            trans = cnmc_inventari.Transformacion()

            identificador = row[0]
            cini = row[1]
            denominacion = row[2]
            codigo_tipo_ct = row[3]
            codigo_ccaa = row[4]
            participacion = row[5]
            fecha_aps = row[6]
            fecha_baja = row[7]

            trans.feed({
                'identificador': '%s' % identificador or ' ',
                'cini': '%s' % cini or ' ',
                'denominacion': '%s' % denominacion or ' ',
                'codigo_tipo_ct': '%s' % codigo_tipo_ct or ' ',
                'codigo_ccaa': '%s' % codigo_ccaa,
                'participacion': '%s' % participacion,
                'fecha_aps': '%s' % fecha_aps or ' ',
                'fecha_baja': '%s' % fecha_baja or ' ',
            })
            self.pla_inversions_xml.linea.append(trans)

    def tractar_centres_transformadors(self):
        self.tractar_trans(self.transformacion)

    def escriure_xml(self, arxiuxml):
        self.pla_inversions_xml.set_xml_encoding('iso-8859-1')
        self.pla_inversions_xml.build_tree()
        arxiuxml.write(str(self.pla_inversions_xml))

    def calc(self):
        try:
            self.check_encoding()
            #Obro l'arxiu XML a emplenar
            arxiuxml = open(self.file_out, 'wb')

            # Carrega dels fitxers CSV LAT
            self.tractar_linies_at()

            # Carrega dels fitxers CSV LBT
            self.tractar_linies_bt()

            # Carrega dels fitxers CSV subestacions
            self.tractar_subestacions()

            # Carrega dels fitxers CSV posicions
            self.tractar_posicions()

            # Carrega dels fitxers CSV maquines
            self.tractar_maquines()

            # Carrega dels fitxers CSV despatxos
            self.tractar_despatxos()

            # Carrega dels fitxers CSV fiabilitat
            self.tractar_equips_fiabilitat()

            # Carrega dels fitxers CSV transformadors
            self.tractar_centres_transformadors()

            # Escrivim l'arxiu XML
            self.escriure_xml(arxiuxml)
        except:
            traceback.print_exc()
            if self.raven:
                self.raven.captureException()