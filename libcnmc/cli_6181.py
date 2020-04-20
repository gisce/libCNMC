# -*- coding: utf-8 -*-
import click
from libcnmc.utils import N_PROC
from libcnmc.core.backend import OOOPFactory
from datetime import datetime


@click.group()
def cnmc_6181():
    pass


def calc_report(process_cls, **kwargs):
    """
    Generates the CSV file for the given process

    :param process_cls: process to generate
    :type process_cls: MultiprocessBased
    :param kwargs: Parameters to generate the file, passed to the process
    :type kwargs: dict(str,str)
    :return: None
    :rtype: None
    """

    O = OOOPFactory(dbname=kwargs['database'], user=kwargs['user'],
                    pwd=kwargs['password'], port=kwargs['port'],
                    uri=kwargs['server'])
    proc = process_cls(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year'],
        prefix=kwargs.get('prefix', False),
        include_header=kwargs.get('include_header', False),
        include_obra=kwargs.get('include_obra', False),
        extended=kwargs.get('extended', False),
    )
    proc.calc()


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('-pf', '--prefix', help="Prefix dels Trams AT")
@click.option('--num-proc', default=N_PROC, type=click.INT)
def audit_6181_lat(**kwargs):
    """
    Click entry to generate LAT(F1) file of 6181

    :param kwargs: Parameters to generate the LAT file
    :type kwargs: dict
    :return: None
    :rtype: None
    """

    from libcnmc.audit_6181 import LAT
    calc_report(LAT, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-pf', '--prefix', help="Prefix dels Trams BT")
def audit_6181_lbt(**kwargs):
    """
    Click entry to generate the LBT(F2) of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.audit_6181 import LBT
    calc_report(LBT, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def audit_6181_se(**kwargs):
    """
    Click entry to generate the SE(F3) file of 6181

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    from libcnmc.audit_6181 import SE
    calc_report(SE, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def audit_6181_pos(**kwargs):
    """
    Click entry to generate the POS(F4) file of 6181

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    from libcnmc.audit_6181 import POS
    calc_report(POS, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def audit_6181_maq(**kwargs):
    """
    Click entry to generate the MAQ(F5 file of 6181

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    from libcnmc.audit_6181 import MAQ
    calc_report(MAQ, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-pf', '--prefix', help="Prefix dels Trams AT")
def audit_6181_fia(**kwargs):
    """
    Click entry to generate the FIA(F7) file of 6181

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """
    from libcnmc.audit_6181 import FIA
    calc_report(FIA, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def audit_6181_cts(**kwargs):
    """
    Click entry to generate the CTs(F8) of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    from libcnmc.audit_6181 import CTS
    calc_report(CTS, **kwargs)


@cnmc_6181.command()
@click.option('-q', '--quiet', default=False,
              help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True,
              help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-h', '--include-header', help='Incloure capçaleres al fitxer', default=False, type=click.BOOL)
@click.option('--include-obra', help='Incloure obres com a ultima columna', default=False, type=click.BOOL)
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def audit_6181_des(**kwargs):
    """
    Click entry to generate the DES(F6) file of 6181

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    from libcnmc.audit_6181 import DES
    calc_report(DES, **kwargs)
