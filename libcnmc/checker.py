# -*- coding: utf-8 -*-
import click
import os
import time

_VERSION = 7.0

_F1 = [
    'Node',
    'Coordenada X',
    'Coordenada Y',
    'Coordenada Z',
    'CNAE',
    'Equip de mesura',
    'Codi de tarifa',
    'Zona de qualitat',
    'CUPS',
    'Codi distribuidora',
    'Municipi',
    'Provincia',
    'Connexió',
    'Tensió d\'alimentació',
    'Estat del contracte',
    'Potència contractada',
    'Potència facturada',
    'Potència adscrita',
    'Energia anual activa',
    'Energia anual reactiva',
    'Any d\'informació'
]

_F1_exclude = [
    0, 1, 2, 3, 12, 19
]

_F1BIS = [
    'CUPS',
    'CINI',
    'Instal·lació',
    'Número de lectures',
    'Canvi de titularitat',
    'Baixa del subministre',
    'Any d\'informació'
]

_F2A = [
    'Codi de mercat',
    'Codi companyia distribuidora declarant',
    'Codi companyia distribuidora client',
    'Codi frontera REE',
    'Coordenada X',
    'Coordenada Y',
    'Tensió'
]

_F2B = [
    'Codi de mercat',
    'Codi companyia distribuidora declarant',
    'Municipi',
    'Provincia'
]

_F10 = [
    'Tram',
    'Node inicial',
    'Node final',
    'CINI',
    'Provincia',
    'Nivell de tensió',
    'Longitud',
    'Número de circuits',
    'Tipus',
    'R',
    'X',
    'Intensitat màxima',
    'Estat operació',
    'Codi companyia distribuidora',
    'Propietat',
    'Any d\'informació'
]


@click.command()
@click.option('-d', '--dir', help='Ruta de la carpeta amb els formularis')
def node_check(**kwargs):
    if not kwargs['dir']:
        print "ERROR: falta especificar la ruta de la carpeta amb els" \
              " formularis."
    else:
        ruta = kwargs['dir']
        if os.path.exists(ruta):
            forms = os.listdir(ruta)
            forms.sort()
            buidar_resultats_anteriors(ruta)
            f11 = None
            f10 = None
            f9 = None
            f1 = None
            for form in forms:
                if '_11_' in form and '~' not in form:
                    f11 = ruta+'/'+form
                if '_10_' in form and '~' not in form:
                    f10 = ruta+'/'+form
                if '_9_' in form and '~' not in form:
                    f9 = ruta+'/'+form
                if '_1_' in form and '~' not in form:
                    f1 = ruta+'/'+form
            if f10:
                for form in forms:
                    ruta_completa = ruta+'/'+form
                    if '_1_' in form and '~' not in form:
                        print "Verificant nodes del formulari 1...\n"
                        nodes = check(f10, ruta_completa, 'f1')
                        mostrar_nodes(nodes, 'f1', ruta)
                        print "Verificant CUPS del formulari 1...\n"
                        cups = check_cups(ruta_completa, 'f1')
                        mostrar_cups(cups, 'f1', ruta)
                        print "Verificant camps obligatoris del formulari " \
                              "1...\n"
                        camps = check_obligatoris(ruta_completa, 'f1')
                        mostrar_obligatoris(camps, 'f1', ruta)
                    elif ('_1bis_' in form or '_1BIS_' in form) \
                            and '~' not in form:
                        print "Verificant CUPS del formulari 1bis...\n"
                        cups = check_cups(ruta_completa, 'f1bis')
                        mostrar_cups(cups, 'f1bis', ruta)
                        cups = check_cups_f1bis(ruta_completa, f1)
                        mostrar_cups_f1bis(cups, ruta)
                        print "Verificant camps obligatoris del formulari " \
                              "1bis...\n"
                        camps = check_obligatoris(ruta_completa, 'f1bis')
                        mostrar_obligatoris(camps, 'f1bis', ruta)
                    elif '_2_' in form and '~' not in form:
                        print "Verificant nodes del formulari 2...\n"
                        nodes = check(f10, ruta_completa, 'f2')
                        mostrar_nodes(nodes, 'f2', ruta)
                        print "Verificant CUPS del formulari 2...\n"
                        cups = check_cups(ruta_completa, 'f2')
                        mostrar_cups(cups, 'f2', ruta)
                    elif ('_2a_' in form or '_2A_' in form) and '~' not in form:
                        print "Verificant camps obligatoris del formulari" \
                              "2a..."
                        camps = check_obligatoris(ruta_completa, 'f2a')
                        mostrar_obligatoris(camps, 'f2a', ruta)
                    elif ('_2b_' in form or '_2B_' in form) and '~' not in form:
                        print "Verificant camps obligatoris del formulari" \
                              "2b..."
                        camps = check_obligatoris(ruta_completa, 'f2b')
                        mostrar_obligatoris(camps, 'f2b', ruta)
                    elif '_3_' in form and '~' not in form:
                        print "Verificant nodes del formulari 3...\n"
                        nodes = check(f10, ruta_completa, 'f3')
                        mostrar_nodes(nodes, 'f3', ruta)
                    elif '_6_' in form and '~' not in form:
                        print "Verificant nodes del formulari 6...\n"
                        nodes = check(f10, ruta_completa, 'f6')
                        mostrar_nodes(nodes, 'f6', ruta)
                        print "Verificant CUPS del formulari 6...\n"
                        cups = check_cups(ruta_completa, 'f6')
                        mostrar_cups(cups, 'f6', ruta)
                    elif '_9_' in form and '~' not in form:
                        print "Verificant trams dels formularis 9 i 10...\n"
                        trams, topologia = check_trams_f9_f10(
                            f10, ruta_completa)
                        mostrar_trams(trams, ruta, 'f9_f10', topologia)
                        print "Verificant trams repetits del formulari " \
                              "9...\n"
                        repetits = comprovar_repetits(ruta_completa, 'f9')
                        mostrar_trams(repetits, ruta, 'f9r')
                    elif '_10_' in form and '~' not in form:
                        print "Verificant trams repetits del formulari 10...\n"
                        repetits = comprovar_repetits(ruta_completa, 'f10')
                        mostrar_trams(repetits, ruta, 'f10r')
                        print "Verificant camps obligatoris del formulari " \
                              "10...\n"
                        camps = check_obligatoris(ruta_completa, 'f10')
                        mostrar_obligatoris(camps, 'f10', ruta)
                    elif '_11_' in form and '~' not in form:
                        print "Verificant nodes del formulari 11...\n"
                        nodes = check(f10, ruta_completa, 'f11')
                        mostrar_nodes(nodes, 'f11', ruta)
                    elif '_12_' in form and '~' not in form:
                        # print "Verificant nodes del formulari 12...\n"
                        # nodes = check(f10, ruta_completa, 'f12')
                        # mostrar_nodes(nodes, 'f12', ruta)
                        if f11:
                            print "Verificant CTs del formulari 12bis...\n"
                            cts = comprovar_cts(f11, ruta_completa, 'f12')
                            mostrar_cts(ruta, cts, 'f12')
                    elif ('_12bis_' in form or '_12BIS_' in form) \
                            and '~' not in form:
                        if f11:
                            print "Verificant CTs del formulari 12bis...\n"
                            cts = comprovar_cts(f11, ruta_completa, 'f12bis')
                            mostrar_cts(ruta, cts, 'f12bis')
                        print "Verificant codis de posició del formulari " \
                              "12bis...\n"
                        posicions = comprovar_repetits(ruta_completa, 'f12bis')
                        mostrar_posicions(ruta, posicions)
                    elif ('_13bis_' in form or '_13BIS_' in form) \
                            and '~' not in form:
                        print "Verificant nodes del formulari 13 bis...\n"
                        nodes = check(f10, ruta_completa, 'f13bis')
                        mostrar_nodes(nodes, 'f13bis', ruta)
                    elif '_15_' in form and '~' not in form:
                        print "Verificant nodes del formulari 15...\n"
                        nodes = check(f10, ruta_completa, 'f15')
                        mostrar_nodes(nodes, 'f15', ruta)
                        if f9:
                            print "Verificant trams del formulari 15 amb els" \
                                  " del formulari 9...\n"
                            trams = check_trams_f15(f9, ruta_completa)
                            mostrar_trams(trams, ruta, 'f15')
                    elif '_16_' in form and '~' not in form:
                        print "Verificant nodes del formulari 16...\n"
                        nodes = check(f10, ruta_completa, 'f16')
                        mostrar_nodes(nodes, 'f16', ruta)
                    elif '_17_' in form and '~' not in form:
                        print "Verificant nodes del formulari 17...\n"
                        nodes = check(f10, ruta_completa, 'f17')
                        mostrar_nodes(nodes, 'f17', ruta)
                    elif '_20_' in form and '~' not in form:
                        print "Verificant CUPS del formulari 20...\n"
                        cups = check_cups(ruta_completa, 'f20')
                        mostrar_cups(cups, 'f20', ruta)
                print "\n\nVerificació completada."
            else:
                print "ERROR: no s'ha trobat el formulari f10 a la" \
                      "carpeta especificada."
        else:
            print "ERROR: No s'ha trobat la carpeta de formularis especificada."


def buidar_resultats_anteriors(ruta):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'w') as f:
        f.write('Resultats de la comprovació dels formularis CNMC 4/2015\n\n')
        f.write('Versió: {0}\n\n'.format(_VERSION))
        f.write('Data de comprovació: {0}\n\n'.format(
            time.strftime("%d/%m/%Y")))
        f.write('#######################################################'
                '\n\n\n\n')


def check_obligatoris(fitxer, context):
    camps = {}
    if existeix_fitxer(fitxer):
        with open(fitxer, 'r') as f:
            j = 1
            for linia in f:
                linia = linia.rstrip()
                camps_linia = linia.split(';')
                i = 0
                for camp in camps_linia:
                    if context == 'f1':
                        cups = camps_linia[8]
                        if camp == '' and i not in _F1_exclude:
                            if cups not in camps.keys():
                                camps[cups] = []
                            camps[cups].append(_F1[i])
                    elif context == 'f1bis':
                        cups = camps_linia[0]
                        if camp == '':
                            if cups not in camps.keys():
                                camps[cups] = []
                            camps[cups].append(_F1BIS[i])
                    elif context == 'f2a':
                        if camp == '':
                            if j not in camps.keys():
                                camps[j] = []
                            camps[j].append(_F2A[i])
                    elif context == 'f2b':
                        if camp == '':
                            if j not in camps.keys():
                                camps[j] = []
                            camps[j].append(_F2B[i])
                    elif context == 'f10':
                        tram = camps_linia[0]
                        if camp == '':
                            if tram not in camps.keys():
                                camps[tram] = []
                            camps[tram].append(_F10[i])
                    i += 1
                j += 1
    return camps


def mostrar_obligatoris(camps, form, ruta):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        if camps:
            f.write("Camps obligatoris del formulari {0} que falten:"
                    "\n".format(form))
            i = 0
            for elem in camps:
                if len(camps[elem]) > 0:
                    f.write("\n")
                    if form == 'f2a' or form == 'f2b':
                        f.write("\tLínia: {0}\n\n".format(elem))
                    elif form == 'f10':
                        if i == 0:
                            f.write("\tLínia: {0}\n\n".format(i+1))
                        else:
                            f.write("\tTram: {0}\n\n".format(elem))
                    else:
                        f.write("\tCUPS: {0}\n\n".format(elem))
                    i += 1
                    for c in camps[elem]:
                        f.write("\t\t{0}\n".format(c))
            f.write("\nTotal de línies amb camps sense emplenar a {0}: "
                    "{1}\n\n".format(form, i))
        else:
            f.write("Tots els camps obligatoris del fitxer {0} estàn complets."
                    " OK!\n\n".format(form))


def mostrar_cups_f1bis(cups, ruta):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        if cups:
            f.write("CUPS del formulari F1 Bis que no apareixen al formulari "
                    "F1:\n\n")
            for elem in cups:
                f.write("\t{0}\n".format(elem))
            f.write("\nTotal CUPS: {0}\n\n".format(len(cups)))
        else:
            f.write("Els cups del formulari F1 Bis coincideixen amb els del"
                    " formulari F1. OK!\n\n")


def check_cups_f1bis(fitxer, f1):
    cups_f1 = []
    cups_f1bis = []
    resultat = []
    if existeix_fitxer(fitxer):
        if existeix_fitxer(f1):
            with open(fitxer) as f1bis:
                for linia in f1bis.readlines():
                    cups_f1bis.append(linia.split(';')[0])
            with open(f1) as f:
                for linia in f.readlines():
                    cups_f1.append(linia.split(';')[8])
            resultat = set(cups_f1bis) - set(cups_f1)
            resultat = list(resultat)
    return resultat


def mostrar_cups(cups, form, ruta):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        if cups:
            f.write("CUPS duplicats del formulari {0}:\n".format(form))
            i = 0
            for elem in cups:
                if len(cups[elem]) > 1:
                    f.write("\n")
                    f.write("\tCodi: {0}\n\n".format(elem))
                    i += 1
                    for c in cups[elem]:
                        f.write("\t\t{0}\n".format(c))
            f.write("\nTotal CUPS duplicats a {0}: {1}\n\n".format(
                form, i))
        else:
            f.write("No hi ha CUPS duplicats al formulari {0}. "
                    "OK!\n\n".format(form))


def check_cups(fitxer, form):
    cups = {}
    cups_repetits = {}
    if existeix_fitxer(fitxer):
        with open(fitxer, 'r') as f:
            for linia in f.readlines():
                if form == 'f1':
                    cups_complet = linia.split(';')[8]
                elif form == 'f1bis':
                    cups_complet = linia.split(';')[0]
                elif form == 'f2':
                    cups_complet = linia.split(';')[5]
                elif form == 'f6':
                    cups_complet = linia.split(';')[6]
                elif form == 'f20':
                    cups_complet = linia.split(';')[1]
                cups_trim = cups_complet[:18]
                if cups_trim in cups.keys():
                    cups_repetits[cups_trim] = cups[cups_trim]
                    cups_repetits[cups_trim].append(cups_complet)
                else:
                    cups[cups_trim] = [cups_complet]
    return cups_repetits


def mostrar_posicions(ruta, posicions):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        if posicions:
            f.write("Posicions duplicades del formulari F12 Bis:\n\n")
            for posicio in posicions:
                f.write("\t{0}\n".format(posicio))
            f.write("\nTotal posicions duplicades F12 Bis: {0}\n\n".format(
                len(posicions)))
        else:
            f.write("Els codis de posició del formulari F12 Bis no estan "
                    "duplicats. OK!\n\n")


def mostrar_cts(ruta, cts, context):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        if cts:
            f.write("CTs del formulari {0} que no apareixen al formulari "
                    "11:\n\n".format(context))
            for ct in cts:
                f.write("\tEl CT {0} no apareix al formulari F11.\n".format(ct))
            f.write("\nTotal CTs {0}: {1}\n\n".format(context, len(cts)))
        else:
            f.write("Els CTs del formulari {0} apareixen a l'f11. "
                    "OK!\n\n".format(context))


def comprovar_cts(ruta_f11, ruta_fitxer, context):
    cts = []
    index = 0
    cts_f11 = []
    cts_fitxer = []
    if context == 'f12':
        index = 1
    if existeix_fitxer(ruta_f11):
        if existeix_fitxer(ruta_fitxer):
            # Carreguem els CTs de l'F11
            with open(ruta_f11, 'r') as f11:
                for linia in f11.readlines():
                    linia = linia.rstrip()
                    ct = linia.split(';')[1]
                    if ct not in cts_f11:
                        cts_f11.append(ct)
            # Comprovem els CTS amb l'F11
            with open(ruta_fitxer, 'r') as fitxer:
                for linia in fitxer.readlines():
                    linia = linia.rstrip()
                    ct = linia.split(';')[index]
                    cts_fitxer.append(ct)
            cts = set(cts_fitxer) - set(cts_f11)
            cts = list(cts)
    return cts


def comprovar_repetits(ruta_fitxer, context):
    trams = []
    repetits = []
    if existeix_fitxer(ruta_fitxer):
        with open(ruta_fitxer, 'r') as fitxer:
            for linia in fitxer.readlines():
                linia = linia.rstrip()
                if context == 'f9':
                    if ';' not in linia and linia != 'END' and linia != '':
                        if linia not in trams:
                            trams.append(linia)
                        else:
                            repetits.append(linia)
                elif context == 'f10':
                    tram = linia.rsplit(';')[0]
                    if tram not in trams:
                        trams.append(tram)
                    else:
                        repetits.append(tram)
                elif context == 'f12bis':
                    posicio = linia.split(';')[2]
                    if posicio not in trams:
                        trams.append(posicio)
                    else:
                        repetits.append(posicio)
    return repetits


def check_trams_f15(ruta_f9, ruta_f15):
    trams = []
    if existeix_fitxer(ruta_f9):
        if existeix_fitxer(ruta_f15):
            trams_f9 = []
            trams_f15 = []
            # llegim tots els trams de l'F9
            with open(ruta_f9, 'r') as f9:
                for linia in f9.readlines():
                    linia = linia.rstrip()
                    if ';' not in linia and linia != 'END' and linia != '':
                        trams_f9.append(linia)
            # llegim tots els trams de l'F15
            with open(ruta_f15, 'r') as f15:
                for linia in f15.readlines():
                    linia = linia.rstrip()
                    trams_f15.append(linia.split(';')[2])
            # comparem trams de f15 si son a f9
            resta = set(trams_f15) - set(trams_f9)
            trams = [(x, 'f9', 'f15') for x in resta]
    return trams


def check_trams_f9_f10(ruta_f10, ruta_f9):
    trams = []
    if existeix_fitxer(ruta_f10):
        if existeix_fitxer(ruta_f9):
            trams_f9 = []
            trams_f10 = []
            topologia = []
            # llegim tots els trams de l'F9
            with open(ruta_f9, 'r') as f9:
                for linia in f9.readlines():
                    linia = linia.rstrip()
                    if linia == '':
                        topologia.append(trams_f9.pop())
                    elif linia and ';' not in linia and linia != 'END':
                        trams_f9.append(linia)
            # llegim tots els trams de l'F10
            with open(ruta_f10, 'r') as f10:
                for linia in f10.readlines():
                    linia = linia.rstrip()
                    if linia:
                        trams_f10.append(linia.split(';')[0])
            # comparem trams de f9 si son a f10
            resta = set(trams_f9) - set(trams_f10)
            trams = [(x, 'f10', 'f9') for x in resta]
            # comparem trams de f10 si son a f9
            resta = set(trams_f10) - set(trams_f9)
            trams += [(x, 'f9', 'f10') for x in resta]
    return trams, topologia


def mostrar_trams(trams, ruta, context, topologia=None):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        repetit = ''
        if trams:
            if context == 'f9_f10':
                f.write("Trams dels formularis F9 i F10:\n\n")
            elif context == 'f15':
                f.write("Trams del formulari F15:\n\n")
            elif context == 'f9r':
                repetit = 'F9'
                f.write("Trams repetits del formulari F9:\n\n")
            elif context == 'f10r':
                repetit = 'F10'
                f.write("Trams repetits del formulari F10:\n\n")
            for tram in trams:
                if 'r' not in context:
                    f.write("\tEl tram {0} del formulari {1} no apareix al "
                            "formulari {2}\n".format(tram[0], tram[2], tram[1]))
                else:
                    f.write("\tEl tram {0} es repeteix al formulari "
                            "{1}\n".format(tram, repetit))
            f.write('\nTotal trams: {0}\n\n'.format(len(trams)))
        else:
            if context == 'f9_f10':
                f.write("Els trams dels formularis f9 i f10 coincideixen entre "
                        "ells. OK!\n\n")
            elif context == 'f15':
                f.write("Els trams del formulari f15 coincideixen amb els del "
                        "formulari f9. OK!\n\n")
            elif context == 'f10r':
                f.write("No es repeteix cap tram al formulari f10. OK!\n\n")
            elif context == 'f9r':
                f.write("No es repeteix cap tram al formulari f9. OK!\n\n")
        if topologia:
            f.write("Trams del formulari F9 fora de topologia:\n\n")
            for tram in topologia:
                f.write("\tTram fora de topologia: {0}\n".format(tram))
            f.write('\nTotal fora topologia: {0}\n\n'.format(len(topologia)))


def mostrar_nodes(nodes, form, ruta):
    fitxer_sortida = ruta+'/resultats.txt'
    with open(fitxer_sortida, 'a') as f:
        if nodes:
            f.write(
                "Nodes del formulari {} que no apareixen al formulari "
                "f10:\n\n".format(form))
            for elem in nodes:
                if form == 'f1' or form == 'f2' or form == 'f6':
                    f.write("\tNode: {0}, CUPS: {1}\n".format(elem[0], elem[1]))
                else:
                    f.write("\t{0}\n".format(elem))
            f.write("\nTotal nodes {0}: {1}\n\n".format(form, len(nodes)))
        else:
            f.write("Els nodes del formulari {0} coincideixen amb els del "
                    "formulari f10. OK!\n\n".format(form))


def check(f10_path, file_path, form):
    nodes = []
    if existeix_fitxer(f10_path):
        if existeix_fitxer(file_path):
            nodes_f10 = []
            with open(f10_path, 'r') as f10:
                node_actual = ''
                for linia_f10 in f10.readlines():
                    linia_f10 = linia_f10.rstrip()
                    if linia_f10 != '':
                        node_actual = linia_f10.split(';')[1]
                        nodes_f10.append(node_actual)
                        node_actual = linia_f10.split(';')[2]
                        nodes_f10.append(node_actual)
            # Ja tenim tots els nodes de F10, ara comprovem que cada node
            # del fitxer a comprovar és dins de F10
            with open(file_path, 'r') as f:
                for linia in f.readlines():
                    if form == 'f1' or form == 'f2' or form == 'f3' \
                            or form == 'f6' or form == 'f11' or form == 'f12' \
                            or form == 'f15' or form == 'f16' or form == 'f17':
                        node_actual = linia.split(';')[0]
                    elif form == 'f13bis':
                        node_actual = linia.split(';')[2]
                    if node_actual not in nodes_f10:
                        cups = 'formulari_sense_cups'
                        if node_actual == '':
                            node_actual = "Node buit"
                        if form == 'f1':
                            cups = linia.split(';')[8]
                        elif form == 'f2':
                            cups = linia.split(';')[3]
                        elif form == 'f6':
                            cups = linia.split(';')[4]
                        if not cups:
                            cups = "Sense CUPS"
                        if cups == 'formulari_sense_cups':
                            nodes.append(node_actual)
                        else:
                            nodes.append((node_actual, cups))
    return nodes


def existeix_fitxer(ruta_fitxer):
    if os.path.exists(ruta_fitxer):
        return True
    else:
        print "El fitxer {0} no existeix.".format(ruta_fitxer)
        return False


if __name__ == '__main__':
    node_check()
