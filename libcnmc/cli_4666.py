# -*- coding: utf-8 -*-
import click
from datetime import datetime
from libcnmc.utils import N_PROC
from cli import res_lat, res_pos2


@click.group()
def cnmc():
    pass


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4666_lat(**kwargs):
    from libcnmc.res_4666 import LAT
    last_year = datetime.now().year - 1
    kwargs["compare_field"] = "4131_entregada_{}".format(last_year)
    res_lat(LAT, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4131_lbt(**kwargs):
    from libcnmc.res_4666 import LBT
    last_year = datetime.now().year-1
    kwargs["compare_field"] = "4131_entregada_{}".format(last_year)
    res_lat(LBT, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4131_cts(**kwargs):
    from libcnmc.res_4666 import CTS
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(CTS, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4666_sub(**kwargs):
    from libcnmc.res_4666 import SUB
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(SUB, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4131_pos(**kwargs):
    from libcnmc.res_4666 import POS, POS_INT
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_pos2(POS, POS_INT, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4131_maq(**kwargs):
    from libcnmc.res_4131 import MAQ
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(MAQ, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4131_des(**kwargs):
    from libcnmc.res_4131 import DES
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(DES, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4666_fia(**kwargs):
    from libcnmc.res_4131 import FIA
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(FIA, **kwargs)


@cnmc.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP',
              type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--embarrats/--no-embarrats', default=False,
              help="Afegir embarrats")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4131_con(**kwargs):
    from libcnmc.res_4666 import CON
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(CON, **kwargs)