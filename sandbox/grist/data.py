import os

from objtypes import RaisedException

def log(*args):
  # print(*args)
  pass


class MemoryColumn(object):
  def __init__(self, col):
    self.col = col
    self.data = []
    # Always initialize to include the special empty record at index 0.
    self.growto(1)

  def drop(self):
    del self.data[:]

  def growto(self, size):
    if len(self.data) < size:
      self.data.extend([self.getdefault()] * (size - len(self.data)))

  def getdefault(self):
    return self.col.type_obj.default
  
  def size(self):
    return len(self.data)
  
  def clear(self):
    self.data = []
    self.growto(1)

  def raw_get(self, row_id):
    try:
      return (self.data[row_id])
    except IndexError:
      return self.getdefault()
    
  def set(self, row_id, value):
    try:
      value = (value)
    except Exception as e:
      log('Unable to marshal value: ', value)

    try:
      self.data[row_id] = value
    except Exception as e:
      self.growto(row_id + 1)
      self.data[row_id] = value

  def iterate(self):
    for i in range(1, len(self.data)):
      yield i, self.raw_get(i)

  def copy_from(self, other_column):
    self.data[:] = other_column.data

  def unset(self, row_id):
    pass

class MemoryDatabase(object):
  __slots__ = ('engine', 'tables')

  def __init__(self, engine):
    self.engine = engine
    self.tables = {}

  def close(self):
    self.engine = None
    self.tables = None
    pass

  def begin(self):
    pass

  def commit(self):
    pass

  def create_table(self, table):
    if table.table_id in self.tables:
      return
    log("Creating table %s" % table.table_id)
    self.tables[table.table_id] = dict()


  def drop_table(self, table):
    if table.detached:
      return
    if table.table_id not in self.tables:
      raise ValueError("Table %s already exists" % table.table_id)
    log("Deleting table %s" % table.table_id)
    del self.tables[table.table_id]


  def rename_table(self, old_table_id, new_table_id):
    if old_table_id not in self.tables:
      raise ValueError("Table %s does not exist" % old_table_id)
    if new_table_id in self.tables:
      raise ValueError("Table %s already exists" % new_table_id)
    log("Renaming table %s to %s" % (old_table_id, new_table_id))
    self.tables[new_table_id] = self.tables[old_table_id]


  def create_column(self, col):
    if col.table_id not in self.tables:
      self.tables[col.table_id] = dict()

    if col.col_id in self.tables[col.table_id]:
      old_one = self.tables[col.table_id][col.col_id]
      if old_one == col:
        raise ValueError("Column %s.%s already exists" % (col.table_id, col.col_id))
      col._data = old_one._data
      col._data.col = col
      if col.col_id == 'group':
        log('Column {}.{} is detaching column {}.{}'.format(col.table_id, col.col_id, old_one.table_id, old_one.col_id))
      old_one.detached = True
      old_one._data = None
    else:
      col._data = MemoryColumn(col)
      # log('Column {}.{} is detaching column {}.{}'.format(self.table_id, self.col_id, old_one.table_id, old_one.col_id))
    # log('Creating column: ', self.table_id, self.col_id)
    self.tables[col.table_id][col.col_id] = col
    col.detached = False

  def drop_column(self, col):
    if col.detached:
      return
    
    tables = self.tables

    if col.table_id not in tables:
      raise Exception('Table not found for column: ', col.table_id, col.col_id)
    
    if col.col_id not in tables[col.table_id]:
      raise Exception('Column not found: ', col.table_id, col.col_id)

    log('Destroying column: ', col.table_id, col.col_id)
    col._data.drop()
    del tables[col.table_id][col.col_id]

import random
import string
import actions
from sql import change_column_type, delete_column, open_connection


class SqlColumn(object):
  def __init__(self, db, col):
    self.db = db
    self.col = col

  def growto(self, size):
    if self.size() < size:
      for i in range(self.size(), size):
        self.set(i, self.getdefault())

  def iterate(self):
    cursor = self.db.sql.cursor()
    try:
      for row in cursor.execute('SELECT id, "{}" FROM "{}" ORDER BY id'.format(self.col.col_id, self.col.table_id)):
        yield row[0], self.col.type_obj.decode(row[1])
    finally:
      cursor.close()


  def copy_from(self, other_column):
    size = other_column.size()
    if size < 2:
      return
    self.growto(other_column.size())
    for i, value in other_column.iterate():
      self.set(i, value)


  def raw_get(self, row_id):
    if row_id == 0:
      return self.getdefault()
    
    table_id = self.col.table_id
    col_id = self.col.col_id
    type_obj = self.col.type_obj

    cursor = self.db.sql.cursor()
    value = cursor.execute('SELECT "{}" FROM "{}" WHERE id = ?'.format(col_id, table_id), (row_id,)).fetchone()
    cursor.close()
    value = value[0] if value else self.getdefault()
    decoded = type_obj.decode(value)
    return decoded


  def set(self, row_id, value):
    try:
      if self.col.col_id == "id" and not value:
        return
      cursor = self.db.sql.cursor()
      encoded = self.col.type_obj.encode(value)
      exists = cursor.execute('SELECT EXISTS(SELECT 1 FROM "{}" WHERE id = ?)'.format(self.col.table_id), (row_id,)).fetchone()[0]
      if not exists:
        cursor.execute('INSERT INTO "{}" (id, "{}") VALUES (?, ?)'.format(self.col.table_id, self.col.col_id), (row_id, encoded))
      else:
        cursor.execute('UPDATE "{}" SET "{}" = ? WHERE id = ?'.format(self.col.table_id, self.col.col_id), (encoded, row_id))
    except Exception as e:
      log("Error setting value: ", row_id, encoded, e)
      raise

  def getdefault(self):
    return self.col.type_obj.default

  def size(self):
    max_id = self.db.sql.execute('SELECT MAX(id) FROM "{}"'.format(self.col.table_id)).fetchone()[0]
    max_id = max_id if max_id is not None else 0
    return max_id + 1

  def create_column(self):
    try:
      cursor = self.db.sql.cursor()
      col = self.col
      if col.col_id == "id":
        pass
      else:
        log('Creating column {}.{} with type {}'.format(self.col.table_id, self.col.col_id, self.col.type_obj.sql_type()))
        if col.col_id == "group" and col.type_obj.sql_type() != "TEXT":
          log("Group column must be TEXT")
        cursor.execute('ALTER TABLE "{}" ADD COLUMN "{}" {}'.format(self.col.table_id, self.col.col_id, self.col.type_obj.sql_type()))
      cursor.close()
    except Exception as e:
      raise

  def clear(self):
    cursor = self.db.sql.cursor()
    cursor.execute('DELETE FROM "{}"'.format(self.col.table_id))
    cursor.close()
    self.growto(1)    

  def drop(self):
    delete_column(self.db.sql, self.col.table_id, self.col.col_id)

  def unset(self, row_id):
    if self.col.col_id != 'id':
      return
    log('Removing row {} from column {}.{}'.format(row_id, self.col.table_id, self.col.col_id))
    cursor = self.db.sql.cursor()
    cursor.execute('DELETE FROM "{}" WHERE id = ?'.format(self.col.table_id), (row_id,))
    cursor.close()


class SqlTable(object):
  def __init__(self, db, table):
    self.db = db
    self.table = table
    self.columns = {}


  def has_column(self, col_id):
    return col_id in self.columns


class SqlDatabase(object):
  def __init__(self, engine):
    self.engine = engine
    while True:
      random_file = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10)) + '.grist'
      random_file = os.path.join('./', random_file)
      # Test if file exists
      if not os.path.isfile(random_file):
        break
    # random_file = ':memory:'
    print('Creating database: ', random_file)
    self.sql = open_connection(random_file)
    self.tables = {}
    self.counter = 0
    self.file = random_file
    self.detached = dict()

  
  def rename_table(self, old_id, new_id):
    orig_ol = old_id
    if old_id.lower() == new_id.lower():
      self.sql.execute('ALTER TABLE "{}" RENAME TO "{}"'.format(old_id, old_id + "_tmp"))
      old_id = old_id + "_tmp"
    self.sql.execute('ALTER TABLE "{}" RENAME TO "{}"'.format(old_id, new_id))
    self.tables[new_id] = self.tables[orig_ol]
    del self.tables[orig_ol]


  def begin(self):
    if self.counter == 0:
      # self.sql.execute('BEGIN TRANSACTION')
      log('BEGIN TRANSACTION')
      pass
    self.counter += 1

  def commit(self):
    self.counter -= 1
    if self.counter < 0:
      raise Exception("Commit without begin")
    if self.counter == 0:
      # self.sql.commit()
      log('COMMIT')
      pass

  def close(self):
    self.sql.close()
    self.sql = None
    self.tables = None

  def read_table(self, table_id):
    return read_table(self.sql, table_id)
  
  def detach_table(self, table):
    table.detached = True

  def create_table(self, table):
    if table.table_id in self.tables:
      return

    cursor = self.sql.cursor()
    log('Creating table: ', table.table_id)
    cursor.execute('CREATE TABLE "' + table.table_id + '" (id INTEGER PRIMARY KEY AUTOINCREMENT)')
    self.tables[table.table_id] = {}

  def create_column(self, col):
    if col.table_id not in self.tables:
      raise Exception("Table {} does not exist".format(col.table_id))

    if col.col_id in self.tables[col.table_id]:
      old_one = self.tables[col.table_id][col.col_id]
      col._data = SqlColumn(self, col)
      if type(old_one.type_obj) != type(col.type_obj):
        # First change name of the column.
        col._data.copy_from(old_one._data)
        change_column_type(self.sql, col.table_id, col.col_id, col.type_obj.sql_type())
      old_one.detached = True
      old_one._data = None
    else:
      col._data = SqlColumn(self, col)
      log('Creating column: ', col.table_id, col.col_id)
      col._data.create_column()
    self.tables[col.table_id][col.col_id] = col
    col.detached = False

  def drop_column(self, col):
    tables = self.tables

    if col.detached or col._table.detached:
      return

    if col.table_id not in tables:
      raise Exception('Cant remove column {} from table {} because table does not exist'.format(col.col_id, col.table_id))
    
    if col.col_id not in tables[col.table_id]:
      raise Exception('Column not found: ', col.table_id, col.col_id)

    log('Destroying column: ', col.table_id, col.col_id)
    col._data.drop()
    del tables[col.table_id][col.col_id]


  def drop_table(self, table):
    if table.table_id in self.detached:
      del self.detached[table.table_id]
      return

    if table.table_id not in self.tables:
      raise Exception('Table not found: ', table.table_id)
    cursor = self.sql.cursor()
    cursor.execute('DROP TABLE ' + table.table_id)
    del self.tables[table.table_id]


def read_table(sql, tableId):
  cursor = sql.cursor()
  cursor.execute('SELECT * FROM ' + tableId)
  data = cursor.fetchall()
  cursor.close()
  rowIds = [row['id'] for row in data]
  columns = {}
  for row in data:
    for key in row.keys():
      if key != 'id':
        if key not in columns:
          columns[key] = []
        columns[key].append(row[key])
  return actions.TableData(tableId, rowIds, columns)


def make_data(eng):
  # return MemoryDatabase(eng)
  return SqlDatabase(eng)