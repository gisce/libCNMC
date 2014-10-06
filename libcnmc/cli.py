# -*- coding: utf-8 -*-
import click
from datetime import datetime

from ooop import OOOP
from libcnmc.utils import N_PROC
from libcnmc.core import UpdateCNMCStats, UpdateCINISComptador
from libcnmc.cir_4_2014 import F1, F1bis, F11
from libcnmc.res_4603 import LAT, LBT, SUB, POS, MAQ, DES, FIA, CTS, INV
from libcnmc.res_4603 import CINIMAQ, CINIPOS, CreateCelles, UpdateCINISTrafo
from libcnmc.res_4603 import UpdateCINISTrams, UpdateCINISCts


@click.group()
def cnmc():
    pass


@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-f',  '--file-input', type=click.Path(exists=True))
def update_cnmc_stats(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = UpdateCNMCStats(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        connection=O,
        num_proc=kwargs['num_proc'],
        file_input=kwargs['file_input']
    )
    proc.execute()

@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-f',  '--file-input', type=click.Path(exists=True))
def update_cinis_comptador(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = UpdateCINISComptador(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        connection=O,
        num_proc=kwargs['num_proc'],
        file_input=kwargs['file_input']
    )
    proc.execute()



@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
def cir_4_2014_f1(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = F1(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()

@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
def cir_4_2014_f1bis(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = F1bis(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
def cir_4_2014_f11(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = F11(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV LAT
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_lat(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = LAT(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()

#CSV LBT
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_lbt(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = LBT(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV SUB
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_sub(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = SUB(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV POS
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_pos(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = POS(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV MAQ
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_maq(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = MAQ(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV DES
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_des(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = DES(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV FIA
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_fia(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = FIA(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV CTS
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_cts(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = CTS(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CSV INV
@cnmc.command()
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option("-l", "--liniesat", help="Fitxers CSV de linies AT",
              type=click.Path(exists=True))
@click.option("-b", "--liniesbt", help="Fitxers CSV de linies BT",
              type=click.Path(exists=True))
@click.option("-e", "--subestacions", help="Fitxers CSV de subestacions",
              type=click.Path(exists=True))
@click.option("-z", "--posicions", help="Fitxers CSV de posicions",
              type=click.Path(exists=True))
@click.option("-m", "--maquines", help="Fitxers CSV de maquines",
              type=click.Path(exists=True))
@click.option("-x", "--despatxos", help="Fitxers CSV de despatxos",
              type=click.Path(exists=True))
@click.option("-f", "--fiabilitat", help="Fitxers CSV de fiabilitat",
              type=click.Path(exists=True))
@click.option("-t", "--transformacio", help="Fitxers CSV de transformacio",
              type=click.Path(exists=True))

def res_4603_inv(**kwargs):
    proc = INV(
        output=kwargs['output'],
        codi_r1=kwargs['codi_r1'],
        liniesat=kwargs['liniesat'],
        liniesbt=kwargs['liniesbt'],
        subestacions=kwargs['subestacions'],
        posicions=kwargs['posicions'],
        maquinas=kwargs['maquines'],
        despatxos=kwargs['despatxos'],
        fiabilidad=kwargs['fiabilitat'],
        transformacion=kwargs['transformacio'],
    )
    proc.calc()


#CINIS Maquines
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_cinimaq(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = CINIMAQ(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


#CINIS Posicions
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
@click.option('--num-proc', default=N_PROC, type=click.INT)
def res_4603_cinipos(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = CINIPOS(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    proc.calc()


@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-f',  '--file-input', type=click.Path(exists=True))
def create_celles(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = CreateCelles(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        connection=O,
        num_proc=kwargs['num_proc'],
        file_input=kwargs['file_input'])
    proc.calc()

@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-f',  '--file-input', type=click.Path(exists=True))
def update_cinis_trafo(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = UpdateCINISTrafo(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        connection=O,
        num_proc=kwargs['num_proc'],
        file_input=kwargs['file_input'])
    proc.calc()

@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-f',  '--file-input', type=click.Path(exists=True))
def update_cinis_trams(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = UpdateCINISTrams(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        connection=O,
        num_proc=kwargs['num_proc'],
        file_input=kwargs['file_input'])
    proc.calc()


@cnmc.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
@click.option('-f',  '--file-input', type=click.Path(exists=True))
def update_cinis_cts(**kwargs):
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    proc = UpdateCINISCts(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        connection=O,
        num_proc=kwargs['num_proc'],
        file_input=kwargs['file_input'])
    proc.calc()


def invoke():
    cnmc()
