import re
import os
import logging

from osconf import config_from_environment
from ooop import OOOP


def camel2dot(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1.\2', name)
    return re.sub('([a-z])([A-Z0-9])', r'\1.\2', s1).lower()


def OOOPFactory(**kwargs):
    logger = logging.getLogger('libCNMC.OOOPFactory')
    try:
        logger.info('Using native OOOP')
        service = OpenERPService()
        service.db_name = kwargs['dbname']
        O = PoolWrapper(service.pool, service.db_name, 1)
    except:
        import traceback
        traceback.print_exc()
        logger.info('Using XML-RPC OOOP')
        O = OOOP(**kwargs)
    return O


class OpenERPService(object):
    def __init__(self, **kwargs):
        import sys
        sys.argv = [sys.argv[0]]
        config = config_from_environment('OPENERP', [], **kwargs)
        import netsvc
        logging.disable(logging.CRITICAL)
        import tools
        for key, value in config.iteritems():
            tools.config[key] = value
        tools.config.parse()
        from tools import config as default_config
        for key, value in config.iteritems():
            default_config[key] = value
        self.config = default_config
        import pooler
        import workflow
        self.pooler = pooler
        self.db = None
        self.pool = None
        if 'db_name' in config:
            self.db_name = config['db_name']

    @property
    def db_name(self):
        return self.config['db_name']

    @db_name.setter
    def db_name(self, value):
        self.config['db_name'] = value
        self.db, self.pool = self.pooler.get_db_and_pool(self.db_name)
        # TODO: Patch ir.cron


class Transaction(object):

    def __init__(self):
        self.database = None
        self.service = None
        self.pool = None
        self.cursor = None
        self.user = None
        self.context = None

    def start(self, database_name, user=1, context=None):
        self._assert_stopped()
        self.service = OpenERPService(db_name=database_name)
        self.pool = self.service.pool
        self.cursor = self.service.db.cursor()
        self.user = user
        self.context = context if context is not None else self.get_context()
        return self

    def stop(self):
        'End the transaction'
        self.cursor.close()
        self.service = None
        self.cursor = None
        self.user = None
        self.context = None
        self.database = None
        self.pool = None

    def get_context(self):
        'Loads the context of the current user'
        assert self.user is not None

        user_obj = self.pool.get('res.users')
        return user_obj.context_get(self.cursor, self.user)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def _assert_stopped(self):
        'Assert that there is no active transaction'
        assert self.service is None
        assert self.database is None
        assert self.cursor is None
        assert self.pool is None
        assert self.user is None
        assert self.context is None


class PoolWrapper(object):
    def __init__(self, pool, dbname, uid):
        self.pool = pool
        self.dbname = dbname
        self.uid = uid
        self.ppid = os.getpid()
        self.fork = False

    def __getattr__(self, name):
        if not self.fork and (os.getpid() != self.ppid):
            import sql_db
            sql_db.close_db(self.dbname)
            service = OpenERPService()
            service.pooler.pool_dic.pop(self.dbname, None)
            service.db_name = self.dbname
            self.pool = service.pool
            self.fork = True
        name = camel2dot(name)
        return self.model(name)

    def model(self, name):
        return ModelWrapper(self.pool.get(name), self.dbname, self.uid)


class ModelWrapper(object):
    def __init__(self, model, dbname, uid):
        self.model = model
        self.dbname = dbname
        self.uid = uid

    def __getattr__(self, item):
        base = getattr(self.model, item)
        if callable(base):
            def wrapper(*args):
                with Transaction().start(self.dbname, user=self.uid) as txn:
                    newargs = (txn.cursor, txn.user) + args
                    return base(*newargs)
            return wrapper
        else:
            return base