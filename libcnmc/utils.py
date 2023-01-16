# -*- coding: utf-8 -*-
import os
import multiprocessing
import tempfile

from shapely import wkt
from shapely.geometry import Point
from shapely.ops import transform
import pyproj

AVAILABLE_CPUS = max(multiprocessing.cpu_count() - os.getloadavg()[0], 0) + 1

N_PROC = max(int(os.getenv('N_PROC', 0)), AVAILABLE_CPUS)

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
    '6.1A': 441,
    '6.1B': 442,
    '6.2': 406,
    '6.2A': 406,
    '6.2B': 406,
    ###
    '2.0TD': 451,
    '3.0TD': 452,
    '6.1TD': 453,
    '6.2TD': 454,
    '6.3TD': 455,
    '6.4TD': 456,
    '3.0TDVE': 471,
    '6.1TDVE': 472
}


CODIS_ZONA = {'RURALDISPERSA': 'RD', 'RURALCONCENTRADA': 'RC',
              'SEMIURBANA': 'SU', 'URBANA': 'U'}


CINI_TG_REGEXP = 'I310.[23]0.$'

TENS_NORM = []

INES = {}

TARIFAS_AT = [
    '3.1A', '3.1A LB', '6.1', '6.1A', '6.1B', '6.2', '6.2A', '6.2B'
]

TARIFAS_BT = [
    '2.0A', '2.0DHA', '2.1A', '2.1DHA', '2.0DHS', '2.1DHS', '3.0A'
]

SUPPORTED_FORMS_CIR_8_2021 = [
    ('a1', 'A1'),
    ('a2', 'A2'),
    ('a3', 'A3'),
    ('a4', 'A4'),
    ('a5', 'A5'),
    ('b1', 'B1'),
    ('b1_1', 'B1.1'),
    ('b2', 'B2'),
    ('b2_1', 'B2.1'),
    ('b2_2', 'B2.2'),
    ('b3', 'B3'),
    ('b3_1', 'B3.1'),
    ('b3_2', 'B3.2'),
    ('b3_3', 'B3.3'),
    ('b4', 'B4'),
    ('b5', 'B5'),
    ('b6', 'B6'),
    ('b7', 'B7'),
    ('b8', 'B8'),
    ('b9', 'B9'),
    ('d2', 'D2')
]

PROJS = {
        '25829': pyproj.CRS('EPSG:25829'),
        '25830': pyproj.CRS('EPSG:25830'),
        '25831': pyproj.CRS('EPSG:25831'),
}

PROJ_TRANSFORMERS = {
        '25829': pyproj.Transformer.from_crs(
            PROJS['25829'], PROJS['25830']
        ).transform,
        '25830': pyproj.Transformer.from_crs(
            PROJS['25830'], PROJS['25830']
        ).transform,
        '25831': pyproj.Transformer.from_crs(
            PROJS['25831'], PROJS['25830']
        ).transform,
}

def get_forced_elements(connection, model):
    """
    Returns the force include and force exclude ids of elements
    :param connection:
    :param model:

    :return:Included and excluded elements
    :rtype: dict(str,list)
    """
    c = connection
    try:
        mod_obj = getattr(c, c.normalize_model_name(model))
    except Exception:
        mod_obj = c.model(model)

    include_search_params = [("criteri_regulatori", "=", "incloure")]
    include_ids = mod_obj.search(
        include_search_params, False, False, False, {'active_test': False}
    )

    include_search_params = [("criteri_regulatori", "=", "excloure")]
    exclude_ids = mod_obj.search(
        include_search_params, False, False, False, {'active_test': False}
    )

    return {
        "include": include_ids,
        "exclude": exclude_ids
    }


def fetch_cts_node(connection):
    """
    Gets the nodes of the CTs

    :param connection: OpenERP connection
    :return: Ct id, node name
    :rtype: dict
    """
    try:
        obj_ct = connection.GiscedataCts
        ids_cts = obj_ct.search([])
        data_cts = obj_ct.read(ids_cts,["node_id"])
        return {data["id"]:data["node_id"] for data in data_cts}
    except Exception:
        ids_blk = connection.GiscegisBlocsCtat.search([])
        read_fields = ["ct", "node"]
        data_cts = connection.GiscegisBlocsCtat.read(ids_blk, read_fields)
        result_dict = dict.fromkeys(ids_blk)
        for ct in data_cts:
            result_dict[ct["ct"][0]] = ct.get("node",['',''])[1]
        return result_dict


def fetch_prov_ine(connection):
    """
    Fetch the INE code of the provincias

    :param connection: OpenERP connection
    :return: provnice id, INE code
    :rtype: dict
    """

    ids_prov = connection.ResCountryState.search([])
    data_prov = connection.ResCountryState.read(ids_prov, ['code'])
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
    return d_out


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
        data = connection.GiscedataTipusInstallacio.read([ti], ["name"])[0]
        if data:
            return data["name"]
        else:
            return ""
    else:
        return ""


def get_codi_actuacio(connection, actuacio):
    """
    Returns the code of the actuacio

    :param connection: Database connection
    :param actuacio: Id of the Actuacio
    :type actuacio: int
    :return: Code of the Actuacio
    :rtype: str
    """
    code = ''
    if actuacio:
        data = connection.GiscedataActuacio.read([actuacio], ["codi"])[0]
        if data:
            code = data["codi"]

    return code


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
    :return: State, ine municipi
    :rtype: tuple
    """
    if not INES:
        ids = connection.ResMunicipi.search([('dc', '!=', False)])
        for municipi in connection.ResMunicipi.read(ids, ['ine', 'dc']):
            INES[municipi['ine']] = municipi['dc']
    # Accedim directament per la clau així si peta rebrem un sentry.
    if ine not in INES:
        state = ''
        ret_ine = ''
    else:
        state = ine[:2]
        ret_ine = ine[2:] + INES[ine]
    return state, ret_ine


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


def format_f_6181(num, decimals=2, float_type=None):
    """
    Formats float with comma decimal separator according to 6181 resolution specs

    :param num:
    :param decimals:
    :param float_type: type of float the BOE defines.
                       It can be either 'euro', which has 2 decimals, or
                       'decimal', which has 3.
    :return:
    """

    if isinstance(float_type, str):
        decimals = 2 if float_type == 'euro' else 3 if float_type == 'decimal' else decimals

    return format_f(num, decimals=decimals)


def format_ccaa_code(ccaa):
    """
    E.g. Catalonia has ccaa code: 09.
    If *ccaa* input is '9', the output will be '09'.
    """
    if not ccaa:
        return ''
    if isinstance(ccaa, int):
        ccaa = str(ccaa)
    return ccaa.zfill(2)


def convert_spanish_date(date):
    from datetime import datetime
    try:
        date_parsed = datetime.strptime(str(date), '%Y-%m-%d')
        date = date_parsed.strftime('%d/%m/%Y')
    except:
        pass
    return date


def convert_srid(srid_source, point):
    """
    Converts the projection of the point from srid_source to 25830
    :param srid_source: Source SRID
    :param point: Point to convert
    :type point: list or tuple
    :return: Converted point
    :rtype: tuple
    """
    assert srid_source in ['25829', '25830', '25831']
    dpoint = Point(point[0], point[1])
    transformer = PROJ_TRANSFORMERS[srid_source]

    dpoint = transform(transformer, dpoint)

    return (dpoint.x, dpoint.y)


def get_srid(c):
    """
    Returns the SRID from the configuration

    :param c: OpenERP connection
    :return: SRID
    :rtype: str
    """

    return str(c.GiscegisBaseGeom.get_srid())


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


def adapt_diff(diff):
    """
    Formats diff to human-readable format
    :param diff: dictionary
    :type diff: dict
    :return: result of the formatted text
    :rtype: str
    """

    a = ""
    for key, value in diff.items():
        a += "[{}] nou:{} antic:{} ".format(key, *value)
    return a


def parse_geom(geom):
    """
    Parse the input Geom to a list of dicts with the coordinates that compose it
    :param geom: Geometry on WKT: 'LINESTRING(X1 Y2,...,Xn Yn)', 'POINT(X Y)'
    :type geom: str
    :return: The Points that compose theinput Geometry
    :rtype list of dict[str,str] or []
    """

    if geom:
        points = [
            {
                'x': coord[0],
                'y': coord[1]
            } for coord in wkt.loads(geom).coords
        ]
    else:
        points = []

    return points


def get_ines(o, ids):
    """
    Gets de INE code of 'provincia' and 'municipi' by its ids
    :param o: OpenERP connection
    :param ids: A single 'provincia' id and 'municipi' id
    :type ids: dict
    :return: The INE code of each id
    :rtype: dict
    """
    res = {'ine_municipi': 0, 'ine_provincia': 0}
    if ids.get('id_municipi', False):
        municipi_dict = o.ResMunicipi.read(ids['id_municipi'][0],
                                           ['ine', 'dc'])
        res['ine_municipi'] = '{0}{1}'.format(municipi_dict['ine'][-3:],
                                              municipi_dict['dc'])
    if ids.get('id_provincia', False):
        res['ine_provincia'] = o.ResCountryState.read(
            ids['id_provincia'][0], ['code']
        )['code']
    return res


def get_tipus_connexio(o, id_escomesa):
    """
    Gets the tipus of connexio of an escomesa. If it's a BT escomesa we
    check the TramBT that suplies it to see if its aerial or underground.
    If it's not a BT escomesa we directly set the tipus of connexio to
    aerial.
    :param id_escomesa: Id of the escomesa
    :type id_escomesa: int
    :return: A or S depending on if the linia that suplies the escomesa
             is aerial or underground
    :rtype: str
    """

    tipus = 'A'
    if 'node_id' in o.GiscedataCupsEscomesa.fields_get().keys() and 'edge_id' in o.GiscedataBtElement.fields_get().keys():
        node_id = o.GiscedataCupsEscomesa.read(
            id_escomesa, ['node_id']
        )['node_id']
        if node_id:
            edge_id = o.GiscegisEdge.search(
                [
                    '|',
                    ('start_node', '=', node_id[0]),
                    ('end_node', '=', node_id[0])
                ]
            )
            if edge_id:
                tram_bt = o.GiscedataBtElement.search(
                    [('edge_id', '=', edge_id[0])]
                )
                if tram_bt:
                    tram_bt = o.GiscedataBtElement.read(
                        tram_bt[0], ['tipus_linia']
                    )
                    if tram_bt['tipus_linia']:
                        tipus = tram_bt['tipus_linia'][1][0]
    else:
        bloc = o.GiscegisBlocsEscomeses.search(
            [('escomesa', '=', id_escomesa)]
        )
        if bloc:
            bloc = o.GiscegisBlocsEscomeses.read(bloc[0], ['node'])
            if bloc['node']:
                node = bloc['node'][0]
                edge_bt = o.GiscegisEdge.search(
                    [
                        '|',
                        ('start_node', '=', node),
                        ('end_node', '=', node),
                        '|',
                        ('layer', 'ilike', self.layer),
                        ('layer', 'ilike', 'EMBARRA%BT%')
                    ]
                )
                edge = o.GiscegisEdge.read(
                    edge_bt[0], ['id_linktemplate']
                )
                if edge['id_linktemplate']:
                    tram_bt = o.GiscedataBtElement.search(
                        [
                            ('name', '=', edge['id_linktemplate'])
                        ]
                    )
                    if tram_bt:
                        tram_bt = o.GiscedataBtElement.read(
                            tram_bt[0], ['tipus_linia']
                        )
                        if tram_bt:
                            if tram_bt['tipus_linia']:
                                tipus = tram_bt['tipus_linia'][1][0]

    return tipus
