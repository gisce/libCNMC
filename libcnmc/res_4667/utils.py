
def get_resum_any_id(connection, any):
    """
    Returnt the id of the resum from the year
     
    :param connection: OOOP Connection 
    :param any: Year
    :type any: int, str 
    :return: id of the resum
    :rtype: int
    """

    search_res = [("anyo", "=", any)]
    ids = connection.GiscedataCnmcPla_inversio.search(search_res)

    search_pla = [("pla_inversio", "=", ids[0])]
    id_resum = connection.GiscedataCnmcResum_any.search(search_pla)
    return id_resum
