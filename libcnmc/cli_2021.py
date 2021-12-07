# -*- coding: utf-8 -*-
import click
from libcnmc.utils import N_PROC
from libcnmc.core.backend import OOOPFactory
from libcnmc import cir_2021
from datetime import datetime


@click.group()
def cnmc_2021():
    pass

@cnmc_2021.command()
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('--derechos/--no-derechos', default=False)
@click.option("--reducir-cups/--no-reducir-cups", default=False)
@click.option("--allow-cna/--no-allow-cna", default=False)
@click.option("--zona_qualitat",default="ct")
def cir_2021_fa1(**kwargs):
    """
    Click entry to generate the FA1 of 2021

    :param kwargs: Params to pass to the process
    :type kwargs: dict(str,str)
    :return: None
    :rtype: None
    """
    O = OOOPFactory(dbname=kwargs['database'], user=kwargs['user'],
                    pwd=kwargs['password'], port=kwargs['port'],
                    uri=kwargs['server'])
    proc = cir_2021.FA1(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year'],
        derechos=kwargs["derechos"],
        reducir_cups=kwargs["reducir_cups"],
        allow_cna=kwargs["allow_cna"],
        zona_qualitat=kwargs["zona_qualitat"]
    )
    proc.calc()


@cnmc_2021.command()
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
@click.option('-p', '--port', default=8069,
              help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('--derechos/--no-derechos', default=False)
@click.option("--reducir-cups/--no-reducir-cups",default=False)
def cir_2021_fa4(**kwargs):
    """
    Click entry to generate the FA4 file of 2021

    :param kwargs: Params to pass to the process
    :type kwargs:dict(str,str)
    :return: None
    :rtype: None
    """
    O = OOOPFactory(dbname=kwargs['database'], user=kwargs['user'],
                    pwd=kwargs['password'], port=kwargs['port'],
                    uri=kwargs['server'])
    proc = cir_2021.FA4(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year'],
        derechos=kwargs['derechos'],
        reducir_cupss=kwargs["reducir_cups"]
    )
    proc.calc()