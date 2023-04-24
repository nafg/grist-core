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
    if self.size() == 1:
      return
    raise NotImplementedError("clear() not implemented for this column type")


  def raw_get(self, row_id):
    try:
      return self.data[row_id]
    except IndexError:
      return self.getdefault()
    
  def set(self, row_id, value):
    try:
      self.data[row_id] = value
    except IndexError:
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


  def create_table(self, table):
    if table.table_id in self.tables:
      raise ValueError("Table %s already exists" % table.table_id)
    print("Creating table %s" % table.table_id)
    self.tables[table.table_id] = dict()


  def drop_table(self, table):
    if table.table_id not in self.tables:
      raise ValueError("Table %s already exists" % table.table_id)
    print("Deleting table %s" % table.table_id)
    del self.tables[table.table_id]


  def create_column(self, col):
    if col.table_id not in self.tables:
      self.tables[col.table_id] = dict()

    if col.col_id in self.tables[col.table_id]:
      old_one = self.tables[col.table_id][col.col_id]
      col._data = old_one._data
      col._data.col = col
      old_one.detached = True
      old_one._data = None
    else:
      col._data = MemoryColumn(col)
      # print('Column {}.{} is detaching column {}.{}'.format(self.table_id, self.col_id, old_one.table_id, old_one.col_id))
    # print('Creating column: ', self.table_id, self.col_id)
    self.tables[col.table_id][col.col_id] = col
    col.detached = False

  def drop_column(self, col):
    tables = self.tables

    if col.table_id not in tables:
      raise Exception('Table not found for column: ', col.table_id, col.col_id)
    
    if col.col_id not in tables[col.table_id]:
      raise Exception('Column not found: ', col.table_id, col.col_id)

    print('Destroying column: ', col.table_id, col.col_id)
    col._data.drop()
    del tables[col.table_id][col.col_id]

import json
import random
import string
import actions
from sql import delete_column, open_connection


class SqlColumn(object):
  def __init__(self, db, col):
    self.db = db
    self.col = col
    self.create_column()

  def growto(self, size):
    if self.size() < size:
      for i in range(self.size(), size):
        self.set(i, self.getdefault())


  def iterate(self):
    cursor = self.db.sql.cursor()
    try:
      for row in cursor.execute('SELECT id, "{}" FROM "{}" ORDER BY id'.format(self.col.col_id, self.col.table_id)):
        yield row[0], row[1] if row[1] is not None else self.getdefault()
    finally:
      cursor.close()

  def copy_from(self, other_column):
    self.growto(other_column.size())
    for i, value in other_column.iterate():
      self.set(i, value)

  def raw_get(self, row_id):
    cursor = self.db.sql.cursor()
    value = cursor.execute('SELECT "{}" FROM "{}" WHERE id = ?'.format(self.col.col_id, self.col.table_id), (row_id,)).fetchone()
    cursor.close()
    correct = value[0] if value else None
    return correct if correct is not None else self.getdefault()

  def set(self, row_id, value):
    if self.col.col_id == "id" and not value:
      return
    # First check if we have this id in the table, using exists statmenet
    cursor = self.db.sql.cursor()
    value = value
    if isinstance(value, list):
      value = json.dumps(value)
    exists = cursor.execute('SELECT EXISTS(SELECT 1 FROM "{}" WHERE id = ?)'.format(self.col.table_id), (row_id,)).fetchone()[0]
    if not exists:
      cursor.execute('INSERT INTO "{}" (id, "{}") VALUES (?, ?)'.format(self.col.table_id, self.col.col_id), (row_id, value))
    else:
      cursor.execute('UPDATE "{}" SET "{}" = ? WHERE id = ?'.format(self.col.table_id, self.col.col_id), (value, row_id))

  def getdefault(self):
    return self.col.type_obj.default

  def size(self):
    max_id = self.db.sql.execute('SELECT MAX(id) FROM "{}"'.format(self.col.table_id)).fetchone()[0]
    max_id = max_id if max_id is not None else 0
    return max_id + 1

  def create_column(self):
    cursor = self.db.sql.cursor()
    col = self.col
    if col.col_id == "id":
      pass
    else:
      cursor.execute('ALTER TABLE "{}" ADD COLUMN "{}" {}'.format(self.col.table_id, self.col.col_id, self.col.type_obj.sql_type()))
    cursor.close()

  def clear(self):
    pass

  def drop(self):
    delete_column(self.db.sql, self.col.table_id, self.col.col_id)

  def unset(self, row_id):
    if self.col.col_id != 'id':
      return
    print('Removing row {} from column {}.{}'.format(row_id, self.col.table_id, self.col.col_id))
    cursor = self.db.sql.cursor()
    cursor.execute('DELETE FROM "{}" WHERE id = ?'.format(self.col.table_id), (row_id,))
    cursor.close()

    


class SqlDatabase(object):
  def __init__(self, engine) -> None:
    self.engine = engine
    random_file = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10)) + '.grist'
    self.sql = open_connection(random_file)
    self.tables = {}

  def read_table(self, table_id):
    return read_table(self.sql, table_id)
  

  def create_table(self, table):
    cursor = self.sql.cursor()
    cursor.execute('CREATE TABLE ' + table.table_id + ' (id INTEGER PRIMARY KEY AUTOINCREMENT)')
    self.tables[table.table_id] = {}


  def create_column(self, col):
    if col.table_id not in self.tables:
      self.tables[col.table_id] = dict()

    if col.col_id in self.tables[col.table_id]:
      old_one = self.tables[col.table_id][col.col_id]
      col._data = old_one._data
      col._data.col = col
      old_one.detached = True
      old_one._data = None
    else:
      col._data = SqlColumn(self, col)
      # print('Column {}.{} is detaching column {}.{}'.format(self.table_id, self.col_id, old_one.table_id, old_one.col_id))
    # print('Creating column: ', self.table_id, self.col_id)
    self.tables[col.table_id][col.col_id] = col
    col.detached = False

  def drop_column(self, col):
    tables = self.tables

    if col.table_id not in tables:
      raise Exception('Table not found for column: ', col.table_id, col.col_id)
    
    if col.col_id not in tables[col.table_id]:
      raise Exception('Column not found: ', col.table_id, col.col_id)

    print('Destroying column: ', col.table_id, col.col_id)
    col._data.drop()
    del tables[col.table_id][col.col_id]


  def drop_table(self, table):
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