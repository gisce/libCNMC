# -*- coding: utf-8 -*-
import os

import click
from libcnmc.utils import N_PROC
from libcnmc.core.backend import OOOPFactory
import tempfile
from datetime import datetime


@click.group()
def cnmc_4131():
    pass


# CSV POS
def res_pos2(proc1, proc2, **kwargs):
    """

    :param proc1: generation proces 1
    :param proc2: generation proces 1
    :param kwargs:
    :return:
    """

    O = OOOPFactory(dbname=kwargs["database"], user=kwargs["user"],
                    pwd=kwargs["password"], port=kwargs["port"],
                    uri=kwargs["server"])
    output = kwargs["output"]
    temp_fd = tempfile.NamedTemporaryFile()

    tmp_out1 = temp_fd.name
    temp_fd.close()
    temp_fd = tempfile.NamedTemporaryFile()
    tmp_out2 = temp_fd.name
    temp_fd.close()

    proc = proc1(
        quiet=kwargs["quiet"],
        interactive=kwargs["interactive"],
        output=tmp_out1,
        connection=O,
        num_proc=kwargs["num_proc"],
        codi_r1=kwargs["codi_r1"],
        year=kwargs["year"],
        embarrats=kwargs["embarrats"],
        compare_field=kwargs["compare_field"]
    )
    proc.calc()

    proc_2 = proc2(
        quiet=kwargs["quiet"],
        interactive=kwargs["interactive"],
        output=tmp_out2,
        connection=O,
        num_proc=kwargs["num_proc"],
        codi_r1=kwargs["codi_r1"],
        year=kwargs["year"],
        embarrats=kwargs["embarrats"],
        compare_field=kwargs["compare_field"]
    )
    proc_2.calc()

    final_out = ""
    with open(tmp_out1, 'r') as fd1:
        final_out += fd1.read()

    with open(tmp_out2, 'r') as fd2:
        final_out += fd2.read()

    with open(output, 'w') as fd_out:
        fd_out.write(final_out)

    os.unlink(tmp_out1)
    os.unlink(tmp_out2)


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


@cnmc_4131.command()
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
def res_4131_lat(**kwargs):
    """
    Click entry to gnerate the LAT of 4131

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """
    from libcnmc.res_4131 import LAT, LAT_2015
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(LAT_2015, **kwargs)
    else:
        last_year = datetime.now().year - 1
        kwargs["compare_field"] = "4131_entregada_{}".format(last_year)
        res_lat(LAT, **kwargs)


@cnmc_4131.command()
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
    """
    Click entry to generate the LBT of 4131

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """
    from libcnmc.res_4131 import LBT, LBT_2015
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(LBT_2015, **kwargs)
    else:
        last_year = datetime.now().year - 1
        kwargs["compare_field"] = "4131_entregada_{}".format(last_year)
        res_lat(LBT, **kwargs)


@cnmc_4131.command()
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
    """
    Click entry to generate the CTs of 4131

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import CTS, CTS_2015
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(CTS_2015, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_lat(CTS, **kwargs)


@cnmc_4131.command()
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
def res_4131_sub(**kwargs):
    """
    Click entry to generate the SUB fil of 4131

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import SUB, SUB_2015
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(SUB_2015, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_lat(SUB, **kwargs)


@cnmc_4131.command()
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
    """
    Click entry to generate the POS file of 4131

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import POS, POS_2015, POS_INT
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(POS_2015, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_pos2(POS, POS_INT, **kwargs)


@cnmc_4131.command()
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
    """
    Click entry to generate the MAQ file of 4131

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import MAQ, MAQ_2015
    if kwargs["year"] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(MAQ_2015, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_lat(MAQ, **kwargs)


@cnmc_4131.command()
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
    """
    Click entry to generate the DES file

    :param kwargs: Params to pas to the process
    :type kwargs: dict(str, str) 
    :return: None
    :rtype: None 
    """

    from libcnmc.res_4131 import DES, DES_2015
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(DES_2015, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_lat(DES, **kwargs)


@cnmc_4131.command()
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
def res_4131_fia(**kwargs):
    """
    Click entry to generate the fiabilitat file of 4131

    :param kwargs: Parameters to generate the file
    :type kwargs: dict(str,str)
    :return: None
    :rtype: None
    """
    from libcnmc.res_4131 import FIA, FIA_2015
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(FIA_2015, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_lat(FIA, **kwargs)


@cnmc_4131.command()
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
    """
    Click entry to generate the file of condensaodrs of 4131

    :param kwargs: Params to generate
    :type kwargs: dict(str, str)
    :return: None
    :rtype: None
    """

    from libcnmc.res_4131 import CON
    if kwargs['year'] == 2015:
        kwargs["compare_field"] = "4771_entregada"
        res_lat(CON, **kwargs)
    else:
        kwargs["compare_field"] = "4131_entregada_{}".format(kwargs["year"])
        res_lat(CON, **kwargs)