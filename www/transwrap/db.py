#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库操作相关接口
"""

import threading, logging, functools, uuid, time

def next_id(t=None):
  '''
  Return next id as 50-char string.

  Args:
    t: unix timestamp, default to None and using time.time().
  '''
  if t is None:
    t = time.time()
  return '%015d%s000' %(int(t*1000), uuid.uuid4().hex)


def _profiling(start, sql=''):
  '''
  记录sql语句执行的时间信息
  '''
  t = time.time() - start
  if t > 0.1:
    logging.warning('[PROFILING] [DB] %s: %s' %(t, sql))
  else:
    logging.info('[PROFILING] [DB] %s: %s' %(t, sql))

# 数据库错误处理
class DBError(Exception):
  pass

class MultiColumnsError(DBError):
  pass


# 数据库连接,直到调用cursor时才创建连接
class _LasyConnection(object):
  def __init__(self):
    self._connection = None

  def cursor(self):
    if self._connection is None:
      connection = engine.connect()
      logging.info('open connection <%s>...' %hex(id(connection)))
      self._connection = connection
    return self._connection.cursor()

  def commit(self):
    return self._connection.commit()

  def rollback(self):
    return self._connection.rollback()

  def cleanup(self):
    '''
    数据库资源清理,调用close()关闭连接
    '''
    if self._connection:
      connection = self._connection
      self._connection = None
      logging.info('close connection <%s>...' %hex(id(connection)))
      connection.close()

# global engine object:
engine = None

# 数据库引擎对象:
class _Engine(object):
  def __init__(self, connect):
    self._connect = connect # 入参conncet是一个函数对象

  def connect(self):
    return self._connect()

# 创建数据库引擎
def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
  import mysql.connector
  global engine
  if engine is not None:
    raise DBError('Engine is already initialized.')
  params = dict(user=user, password=password, database=database, host=host,
                port=port)
  # 数据库连接默认参数
  defaults = dict(use_unicode=True, autocommit=False)
  for k, v in defaults.iteritems():
    params[k] = kw.pop(k, v)    # 用户参数覆盖默认参数
  params.update(kw)
  params['buffered'] = True   # buffered cursor

  # 创建数据库引擎
  engine = _Engine(lambda: mysql.connector.connect(**params))
  logging.info('Init mysql engine <%s> ok.' %hex(id(engine)))


# 持有数据库连接的上下文对象:
class _DbCtx(threading.local):
  def __init__(self):
    self._connection = None   # 持有的数据库连接
    self._transactions = 0    # 用于数据库事务控制

  def is_init(self):
    return not self._connection is None

  def init(self):
    logging.info('open lazy connection...')
    self._connection = _LasyConnection()
    self._transactions = 0

  def cleanup(self):
    self._connection.cleanup()
    self._transactions = None

  def cursor(self):
    return self._connection.cursor(dictionary=True)   # dictionary参数使得查询结果默认返回dict

# thread-local db context
_db_ctx = _DbCtx()

# 数据库连接上下文对象:
class _ConnectionCtx(object):
  '''
  _ConnectionCtx object that can open and close connection context.
  定义 __enter__() 和 __exit__() 方法使得可以使用with语法
  '''
  def __enter__(self):
    global _db_ctx    # 函数内想要修改全局变量的值需要使用global声明
    self._should_cleanup = False    # 是否需要清理资源
    if not _db_ctx.is_init():
      _db_ctx.init()
      self._should_cleanup = True
    return self

  def __exit__(self, exctype, excvalue, traceback):
    '''
    异常相关的参数并未使用
    '''
    global _db_ctx
    if self._should_cleanup:
      _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

# decorator
def with_connection(func):
    '''
    Decorator for reuse db connection.

    @with_connection
    def foo(*args, **kw):
        f1()
        f2()
        f3()
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

# 数据库事务控制
class _TransactionCtx(object):
  '''
  _TransactionCtx object that can handle transactions.

  with _TransactionCtx():
    pass
  '''

  def __enter__(self):
    global _db_ctx
    self._should_close_conn = False
    if not _db_ctx.is_init():
      _db_ctx.init()
      self._should_close_conn = True
    _db_ctx._transactions = _db_ctx._transactions + 1
    logging.info('begin transaction...'
                  if _db_ctx._transactions==1 else 'join current transaction...')
    return self

  def __exit__(self, exctype, excvalue, traceback):
    global _db_ctx
    _db_ctx._transactions = _db_ctx._transactions - 1
    try:
      if _db_ctx._transactions == 0:
        if exctype is None:   # 无异常
          self.commit()
        else:
          self.rollback()
    finally:
      if self._should_close_conn:
        _db_ctx.cleanup()

  def commit(self):
    global _db_ctx
    logging.info('commit transaction...')
    try:
      _db_ctx._connection.commit()
      logging.info('commit ok')
    except:
      logging.warning('commit failed. try rollback...')
      _db_ctx._connection.rollback()
      logging.warning('rollback ok.')
      raise   # rollback后将异常抛出

  def rollback(self):
    global _db_ctx
    logging.warning('rollback transaction...')
    _db_ctx._connection.rollback()
    logging.info('rollback ok.')

def transaction():
  '''
  Create a transaction object.
  '''
  return _TransactionCtx()

def with_transaction(func):
  @functools.wraps(func)
  def _wrapper(*args, **kw):
    _start = time.time()
    with _TransactionCtx():
      return func(*args, **kw)
    _profiling(_start)
  return _wrapper

# 实现数据库查询方法
def _select(sql, first, *args):
  global _db_ctx
  cursor = None
  sql = sql.replace('?', '%s')    # 传入的参数是用?作占位符
  logging.info('SQL: %s, ARGS: %s' %(sql, args))
  try:
    cursor = _db_ctx._connection.cursor()
    cursor.execute(sql, args)
    # if cursor.description:    # 获得每个字段的描述,包括字段名,是否为空,类型等
    # names = [x[0] for x in cursor.description]  # 所有字段名的list,为啥不用cursor.column_names?
    if cursor.column_names:
      names = cursor.column_names
    if first:   # 只取第一行
      values = cursor.fetchone()
      if not values:
        return None
      return values   #dict(zip(names, values))   # cursor参数dictionary=True的返回结果就是dict
    return cursor.fetchall()
  finally:
    if cursor:
      cursor.close()

# select方法
@with_connection
def select_one(sql, *args):
  return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
  d = _select(sql, True, *args)
  if len(d) != 1:
    raise MultiColumnsError('Except only one column.')
  return d.values()[0]

@with_connection
def select(sql, *args):
  return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
  global _db_ctx
  cursor = None
  sql = sql.replace('?', '%s')
  logging.info('SQL: %s, ARGS: %s' %(sql, args))
  try:
    cursor = _db_ctx._connection.cursor()
    cursor.execute(sql, args)
    r = cursor.rowcount
    if _db_ctx._transactions == 0:
      logging.info('auto commit')
      _db_ctx._connection.commit()
    return r
  finally:
    if cursor:
      cursor.close()

@with_connection
def insert(table, **kw):
  cols, args = zip(*kw.iteritems())
  sql = ('insert into `%s` (%s) values (%s)'
          %(table, ','.join(['`%s`' %col for col in clos]),
           ','.join(['`%s`' %'?' for i in range(len(cols))])))
  return _update(sql, *args)

@with_connection
def update(sql, *args):
  return _update(sql, *args)
