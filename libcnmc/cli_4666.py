# -*- coding: utf-8 -*-
import click
from libcnmc.utils import N_PROC
from libcnmc.core.backend import OOOPFactory
import tempfile
from datetime import datetime


@click.group()
def cnmc_4666():
    pass


# CSV LAT
def res_lat(LAT, **kwargs):
    """
    Generates the CSV file for LAT process

    :param LAT: process to generate
    :type LAT: MultiprocessBased
    :param kwargs: Parameters to generate the file, passed to the process
    :type kwargs: dict(str,str)
    :return: None
    :rtype: None
    """

    O = OOOPFactory(dbname=kwargs['database'], user=kwargs['user'],
                    pwd=kwargs['password'], port=kwargs['port'],
                    uri=kwargs['server'])
    proc = LAT(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year'],
        embarrats=kwargs['embarrats'],
        compare_field=kwargs["compare_field"]
    )
    proc.calc()


def res_mod(procs, **kwargs):
    """
    Generates the file of modificacions from a list of process

    :param procs:process to generate
    :type procs: list
    :param kwargs: Parameters to generate the file, passed to the process
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    O = OOOPFactory(dbname=kwargs['database'], user=kwargs['user'],
                    pwd=kwargs['password'], port=kwargs['port'],
                    uri=kwargs['server'])

    with open(kwargs["output"], "w") as fd:
        for proc_fnc in procs:
            temp_fd = tempfile.NamedTemporaryFile()
            tmp_url = temp_fd.name

            proc = proc_fnc(
                quiet=kwargs["quiet"],
                interactive=kwargs["interactive"],
                output=tmp_url,
                connection=O,
                num_proc=kwargs["num_proc"],
                year=kwargs["year"],
                explain=kwargs["explain"]
            )
            proc.calc()
            with open(tmp_url, "r") as fd_tmp:
                tmp_data = fd_tmp.read()
            fd.write(tmp_data)


@cnmc_4666.command()
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
@click.option('--explain/--no-explain', default=False,
              help="Explicar resultats")
def res_4666_mod(**kwargs):
    """
    Click entry to generate the modification file for 4666

    :param kwargs: generation arguments
    :type kwargs: dict
    :return: None
    :rtype: None
    """

    from libcnmc.res_4666 import ModCts, ModFia, ModLat, ModPos
    from libcnmc.res_4666 import ModLbt, ModMaq
    procs = [ModCts, ModFia, ModLat, ModLbt, ModMaq, ModPos]
    res_mod(procs, **kwargs)


@cnmc_4666.command()
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
    """
    Click entry to generate LAT file of 4666

    :param kwargs: Parameters to generate the LAT file
    :type kwargs: dict
    :return: None
    :rtype: None
    """

    from libcnmc.res_4666 import LAT
    last_year = datetime.now().year - 1
    kwargs["compare_field"] = "4131_entregada_{}".format(last_year)
    res_lat(LAT, **kwargs)


@cnmc_4666.command()
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
def res_4666_lbt(**kwargs):
    """
    Click entry to generate the LBT of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4666 import LBT
    last_year = datetime.now().year - 1
    kwargs["compare_field"] = "4131_entregada_{}".format(last_year)
    res_lat(LBT, **kwargs)


@cnmc_4666.command()
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
def res_4666_cts(**kwargs):
    """
    Click entry to generate the CTs of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4666 import CTS
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(CTS, **kwargs)


@cnmc_4666.command()
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
    """
    Click entry to generate the CTs file of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4666 import SUB
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(SUB, **kwargs)


@cnmc_4666.command()
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
def res_4666_pos(**kwargs):
    """
    Click entry to generate the POS file of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4666 import POS, POS_INT
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_pos2(POS, POS_INT, **kwargs)


@cnmc_4666.command()
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
def res_4666_maq(**kwargs):
    """
    Click entry to generate the MAQ file of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import MAQ
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(MAQ, **kwargs)


@cnmc_4666.command()
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
def res_4666_des(**kwargs):
    """
    Click entry to generate the DES file of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import DES
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(DES, **kwargs)


@cnmc_4666.command()
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
    """
    Click entry to generate the FIA file of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """
    from libcnmc.res_4131 import FIA
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(FIA, **kwargs)


@cnmc_4666.command()
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
def res_4666_con(**kwargs):
    """
    Click entry to generate the CON file of 4666

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4666 import CON
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(CON, **kwargs)


@cnmc_4666.command()
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
def res_466_con(**kwargs):
    from libcnmc.res_4666 import CON
    kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
    res_lat(CON, **kwargs)
