# -*- coding: utf-8 -*-
import os
import multiprocessing
from pyproj import Proj
from pyproj import transform


N_PROC = int(os.getenv('N_PROC', multiprocessing.cpu_count() + 1))


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
    '6.2A': 406,
}


CODIS_ZONA = {'RURALDISPERSA': 'RD', 'RURALCONCENTRADA': 'RC',
              'SEMIURBANA': 'SU', 'URBANA': 'U'}


CINI_TG_REGEXP = 'I310[12]3.$'

TENS_NORM = []

INES = {}


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
        O = connection
        comp_obj = O.GiscedataLecturesComptador
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
    #Si no hi ha ct agafem la comunitat del rescompany
    company_partner = O.ResCompany.read(1, ['partner_id'])
    #funció per trobar la ccaa desde el municipi
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
    :type point: tuple
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
