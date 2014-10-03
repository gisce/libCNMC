import os
import multiprocessing


N_PROC = int(os.getenv('N_PROC', multiprocessing.cpu_count() + 1))


CODIS_TARIFA = {'2.0A': 416, '2.0DHA': 417, '2.1A': 418, '2.1DHA': 419,
                '2.0DHS': 426, '2.1DHS': 427, '3.0A': 403, '3.1A': 404,
                '3.1A LB': 404,  '6.1': 405}


CODIS_ZONA = {'RURALDISPERSA': 'RD', 'RURALCONCENTRADA': 'RC',
              'SEMIURBANA': 'SU', 'URBANA': 'U'}


CINI_TG_REGEXP = 'I310[12]3.$'


INES = {}


def get_ine(connection, ine):
    """Retornem dos valors el codi de l'estat i el codi ine sense estat.
    """
    if not INES:
        ids = connection.ResMunicipi.search([('dc', '!=', False)])
        for municipi in connection.ResMunicipi.read(ids, ['ine', 'dc']):
            INES[municipi['ine']] = municipi['dc']
    # Accedim directament per la clau així si peta rebrem un sentry.
    state = ine[:2]
    ine = ine[2:] + INES[ine]
    return state, ine


def get_comptador(connection, polissa_id, year):
        O = connection
        comp_obj = O.GiscedataLecturesComptador
        comp_id = comp_obj.search([
            ('polissa', '=', polissa_id),
            ('data_alta', '<', '%s-01-01' % (year + 1))
        ], 0, 1, 'data_alta desc', {'active_test': False})
        return comp_id


def get_id_expedient(connection, expedients_id):
    id_expedient = False
    if expedients_id:
        search_params = [
            ('id', 'in', expedients_id)]
        id_expedients = connection.GiscedataExpedientsExpedient.search(
            search_params, 0, 1, 'industria_data desc')
        if id_expedients:
            id_expedient = id_expedients[0]
    return id_expedient
