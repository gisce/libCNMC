# -*- coding: utf-8 -*-
import os
import multiprocessing
import tempfile

from pyproj import Proj
from pyproj import transform

N_PROC = int(os.getenv('N_PROC', multiprocessing.cpu_count() + 1))

from libcnmc.core.backend import OOOPFactory


CODIS_TARIFA = {
    '2.0A': 416,
    '2.0DHA': 417,
    '2.1A': 418,
    '2.1DHA': 419,
    '2.0DHS': 426,
    '2.1DHS': 427,
    '3.0A': 403,
    '3.1A': 404,
    '3.1A LB': 404,
    '6.1': 405,
    '6.1A': 405,
    '6.1B': 405,
    '6.2': 406,
    '6.2A': 406,
    '6.2B': 406
}


CODIS_ZONA = {'RURALDISPERSA': 'RD', 'RURALCONCENTRADA': 'RC',
              'SEMIURBANA': 'SU', 'URBANA': 'U'}


CINI_TG_REGEXP = 'I310[12]3.$'

TENS_NORM = []

INES = {}


def fetch_cts_node(connection):
    """
    Gets the nodes of the CTs

    :param connection: OpenERP connection
    :return: Ct id, node name
    :rtype: dict
    """

    ids_blk = connection.GiscegisBlocsCtat.search([])
    read_fields = ["ct", "node"]
    data_blk = connection.GiscegisBlocsCtat.read(ids_blk, read_fields)
    result_dict = dict.fromkeys(ids_blk)
    for linia in data_blk:
        result_dict[linia["ct"][0]] = linia["node"][1]
    return result_dict


def fetch_prov_ine(connection):
    """
    Fetch the INE code of the provincias

    :param connection: OpenERP connection
    :return: provnice id, INE code
    :rtype: dict
    """

    ids_prov = connection.ResCountryState.search([])
    data_prov = connection.ResCountryState.read(ids_prov, ['code'])['code']
    dict_prov = dict.fromkeys(ids_prov)
    for prov in data_prov:
        dict_prov[prov["id"]] = prov["code"]
    return dict_prov


def fetch_mun_ine(connection):
    """
    Fetch teh INE code of the municipio

    :param connection:
    :return:
    :rtype: dict
    """

    ids_mun = connection.ResMunicipi.search([])

    data_mun = connection.ResMunicipi.read(ids_mun, ['ine', 'dc'])
    ret_vals = dict.fromkeys(ids_mun)
    for mun in data_mun:
        ret_vals[mun["id"]] = '{0}{1}'.format(mun['ine'][-3:], mun['dc'])
    return ret_vals


def fetch_tensions_norm(connection):
    """
    Gets all the tensions

    :param connection:
    :return: Id and tensio
    :rtype: dict
    """
    t_ids = connection.GiscedataTensionsTensio.search([])
    t_data = connection.GiscedataTensionsTensio.read(t_ids, ["tensio"])
    d_out = dict.fromkeys(t_ids)
    for line in t_data:
        d_out[line["id"]] = format_f(float(line["tensio"]) / 1000.0, decimals=3)


def get_norm_tension(connection, tension):
    """
    Returns the tension normalizada from a given tensio
    
    :param connection: OpenERP connection  
    :param tension: Tensio
    :return: Tensio normalitzada 
    """

    if not TENS_NORM:
        tension_fields_to_read = ['l_inferior', 'l_superior', 'tensio']
        tension_vals = connection.GiscedataTensionsTensio.read(
            connection.GiscedataTensionsTensio.search([]),
            tension_fields_to_read)
        TENS_NORM.extend([(t['l_inferior'], t['l_superior'], t['tensio'])
                          for t in tension_vals])
    if not tension:
        return tension

    for t in TENS_NORM:
        if t[0] <= tension < t[1]:
            return t[2]

    return tension


def get_name_ti(connection, ti):
    """
    Returns the name of the TI
    
    :param connection: Database connection
    :param ti: Id of the TI
    :type ti: int
    :return: Name of the TI
    :rtype: str
    """
    if ti:
        data = connection.GiscedataTipusInstallacio.read(ti, ["name"])
        if data:
            return data["name"]
        else:
            return ""
    else:
        return ""


def get_codigo_ccaa(connection, ccaa):
    """
    Return the code from CCAA
    
    :param connection: Database conection
    :param ccaa: Id of the CCAA
    :return: Codigo CCAA
    :rtype: int, str
    """
    if ccaa:
        data = connection.ResComunitat_autonoma.read(ccaa, ["codi"])
        if data:
            return data["codi"]
        else:
            return ''
    else:
        return ''


def get_ine(connection, ine):
    """
    Retornem dos valors el codi de l'estat i el codi ine sense estat.
    
    :param connection: OpenERP connection
    :param ine:
    :return:
    :rtype: tuple
    """
    if not INES:
        ids = connection.ResMunicipi.search([('dc', '!=', False)])
        for municipi in connection.ResMunicipi.read(ids, ['ine', 'dc']):
            INES[municipi['ine']] = municipi['dc']
    # Accedim directament per la clau així si peta rebrem un sentry.
    if ine not in INES:
        state = ''
        ine = ''
    else:
        state = ine[:2]
        ine = ine[2:] + INES[ine]
    return state, ine


def get_comptador(connection, polissa_id, year):
        comp_obj = connection.GiscedataLecturesComptador
        comp_id = comp_obj.search([
            ('polissa', '=', polissa_id),
            ('data_alta', '<', '{}-01-01'.format(year + 1))
        ], 0, 1, 'data_alta desc', {'active_test': False})
        return comp_id


def get_id_expedient(connection, expedients_id):
    id_expedient = False
    if expedients_id:
        search_params = [
            ('id', 'in', expedients_id), ('industria_data', '!=', False)]
        id_expedients = connection.GiscedataExpedientsExpedient.search(
            search_params, 0, 1, 'industria_data desc')
        if id_expedients:
            id_expedient = id_expedients[0]
    return id_expedient


def get_id_municipi_from_company(connection, get_name=False):
    O = connection
    id_municipi = False
    # Si no hi ha ct agafem la comunitat del rescompany
    company_partner = O.ResCompany.read(1, ['partner_id'])
    # funció per trobar la ccaa desde el municipi
    if company_partner:
        partner_address = O.ResPartner.read(
            company_partner['partner_id'][0], ['address'])
        address = O.ResPartnerAddress.read(
            partner_address['address'][0], ['id_municipi'])
        if address['id_municipi']:
            id_municipi = address['id_municipi'][0]
        if get_name:
            name_municipi = id_municipi = address['id_municipi'][1]
            return  id_municipi, name_municipi
    return id_municipi


def tallar_text(text, longitud):
    try:
        if len(text) > longitud:
            return text[:longitud-3] + '...'
        else:
            return text
    except TypeError:
        return ''


def format_f(num, decimals=2):
    """
    Formats float with comma decimal separator
    
    :param num: 
    :param decimals: 
    :return: 
    """

    if isinstance(num, float):
        fstring = '%%.%df' % decimals
        return (fstring % num).replace('.', ',')
    return num


def convert_srid(codi, srid_source, point):
    """
    Converts the projection of the point from srid_source to 25830

    :param codi: Company code
    :type codi: str
    :param srid_source: Source SRID
    :param point: Point to convert
    :type point: list
    :return: Converted point
    :rtype: tuple
    """
    assert srid_source in ['25829', '25830', '25831']
    if codi == '056':
        return point
    else:
        if srid_source == '25830':
            return point
        else:
            source = Proj(init='epsg:{0}'.format(srid_source))
            dest = Proj(init='epsg:25830')
            result_point = transform(source, dest, point[0], point[1])
            return result_point


def get_srid(connection):
    """
    Returns the SRID from the configuration
    
    :param connection: OpenERP connection  
    :return: SRID
    :rtype: str
    """

    giscegis_srid_id = connection.ResConfig.search(
                    [('name', '=', 'giscegis_srid')])
    giscegis_srid = connection.ResConfig.read(giscegis_srid_id)[0]['value']
    return giscegis_srid


def merge_procs(procs, **kwargs):
    """
    Generates multiple procs and merges the results

    :param procs: Procs to generate
    :type procs: list[MultiprocessBased]
    :param database: OpenERP Database
    :type database: str
    :param user: OpenERP user
    :type user: str
    :param password: OpenERP password
    :type pasword: str
    :param port: OpenERP port
    :param server: OpenERP server to connet
    :type server: str
    :return: Result of the procs
    :rtype: str
    """

    O = OOOPFactory(dbname=kwargs['database'], user=kwargs['user'],
                    pwd=kwargs['password'], port=kwargs['port'],
                    uri=kwargs['server'])

    data_out = ""
    original_out_url = kwargs["output"]
    for proc_fnc in procs:
        temp_fd = tempfile.NamedTemporaryFile()
        tmp_url = temp_fd.name
        temp_fd.close()

        proc_kwargs = kwargs
        proc_kwargs["connection"] = O
        proc_kwargs["output"] = tmp_url
        proc = proc_fnc(**proc_kwargs)
        proc.calc()

        with open(tmp_url, 'r') as fd:
            data_out += fd.read()

    with open(original_out_url, "w") as fd_out:
        fd_out.write(data_out)
