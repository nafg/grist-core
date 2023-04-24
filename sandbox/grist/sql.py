import json
import marshal
import sqlite3
import actions
import six


import sqlite3


def change_id_to_primary_key(conn, table_name):
  cursor = conn.cursor()
  cursor.execute('PRAGMA table_info("{}");'.format(table_name))
  columns = cursor.fetchall()
  create_table_sql = 'CREATE TABLE "{}_temp" ('.format(table_name)
  for column in columns:
      column_name, column_type, _, _, _, _ = column
      primary_key = "PRIMARY KEY" if column_name == "id" else ""
      create_table_sql += '"{}" {} {}, '.format(column_name, column_type, primary_key)
  create_table_sql = create_table_sql.rstrip(", ") + ");"
  cursor.execute(create_table_sql)
  cursor.execute('INSERT INTO "{}_temp" SELECT * FROM "{}";'.format(table_name, table_name))
  cursor.execute('DROP TABLE "{}";'.format(table_name))
  cursor.execute('ALTER TABLE "{}_temp" RENAME TO "{}";'.format(table_name, table_name))
  cursor.close()


def delete_column(conn, table_name, column_name):
  cursor = conn.cursor()
  cursor.execute('PRAGMA table_info("{}");'.format(table_name))
  columns_info = cursor.fetchall()
  new_columns = ", ".join(
      '"{}" {}'.format(col[1], col[2])
      for col in columns_info
      if col[1] != column_name
  )
  if new_columns:
    cursor.execute('CREATE TABLE "new_{}" ({})'.format(table_name, new_columns))
    cursor.execute('INSERT INTO "new_{}" SELECT {} FROM "{}"'.format(table_name, new_columns, table_name))
    cursor.execute('DROP TABLE "{}"'.format(table_name))
    cursor.execute('ALTER TABLE "new_{}" RENAME TO "{}"'.format(table_name, table_name))
  conn.commit()


def rename_column(conn, table_name, old_column_name, new_column_name):
  cursor = conn.cursor()
  cursor.execute('PRAGMA table_info("{}");'.format(table_name))
  columns_info = cursor.fetchall()

  # Construct new column definitions string
  new_columns = []
  for col in columns_info:
      if col[1] == old_column_name:
          new_columns.append('"{}" {}'.format(new_column_name, col[2]))
      else:
          new_columns.append('"{}" {}'.format(col[1], col[2]))
  new_columns_str = ", ".join(new_columns)

  # Create new table with renamed column
  cursor.execute('CREATE TABLE "new_{}" ({});'.format(table_name, new_columns_str))
  cursor.execute('INSERT INTO "new_{}" SELECT {} FROM "{}";'.format(table_name, new_columns_str, table_name))

  # Drop original table and rename new table to match original table name
  cursor.execute('DROP TABLE "{}";'.format(table_name))
  cursor.execute('ALTER TABLE "new_{}" RENAME TO "{}";'.format(table_name, table_name))

  conn.commit()



def change_column_type(conn, table_name, column_name, new_type):
  cursor = conn.cursor()
  cursor.execute('PRAGMA table_info("{}");'.format(table_name))
  columns_info = cursor.fetchall()
  old_type = new_type
  for col in columns_info:
    if col[1] == column_name:
      old_type = col[2].upper()
      break
  if old_type == new_type:
    return
  new_columns = ", ".join(
      '"{}" {}'.format(col[1], new_type if col[1] == column_name else col[2])
      for col in columns_info
  )
  cursor.execute('CREATE TABLE "new_{}" ({});'.format(table_name, new_columns))
  cursor.execute('INSERT INTO "new_{}" SELECT * FROM "{}";'.format(table_name, table_name))
  cursor.execute('DROP TABLE "{}";'.format(table_name))
  cursor.execute('ALTER TABLE "new_{}" RENAME TO "{}";'.format(table_name, table_name))
  conn.commit()


def is_primitive(value):
  string_types = six.string_types if six.PY3 else (str,)
  numeric_types = six.integer_types + (float,)
  bool_type = (bool,)
  return isinstance(value, string_types + numeric_types + bool_type)

def size(sql: sqlite3.Connection, table):
  cursor = sql.execute('SELECT MAX(id) FROM %s' % table)
  value = (cursor.fetchone()[0] or 0)
  return value

def next_row_id(sql: sqlite3.Connection, table):
  cursor = sql.execute('SELECT MAX(id) FROM %s' % table)
  value = (cursor.fetchone()[0] or 0) + 1
  return value

def create_table(sql, table_id):
  sql.execute("CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY)".format(table_id))

def column_raw_get(sql, table_id, col_id, row_id):
  value = sql.execute('SELECT "{}" FROM {} WHERE id = ?'.format(col_id, table_id), (row_id,)).fetchone()
  return value[col_id] if value else None

def column_set(sql, table_id, col_id, row_id, value):
  if col_id == 'id':
    raise Exception('Cannot set id')

  if isinstance(value, list):
    value = json.dumps(value)

  if not is_primitive(value) and value is not None:
    value = repr(value)
  
  try:
    id = column_raw_get(sql, table_id, 'id', row_id)
    if id is None:
      # print("Insert [{}][{}][{}] = {}".format(table_id, col_id, row_id, value))
      sql.execute('INSERT INTO {} (id) VALUES (?)'.format(table_id), (row_id,))
    else:
      # print("Update [{}][{}][{}] = {}".format(table_id, col_id, row_id, value))
      pass
    sql.execute('UPDATE {} SET "{}" = ? WHERE id = ?'.format(table_id, col_id), (value, row_id))
  except sqlite3.OperationalError:
    raise

def column_grow(sql, table_id, col_id):
  sql.execute("INSERT INTO {} DEFAULT VALUES".format(table_id, col_id))

def col_exists(sql, table_id, col_id):
  cursor = sql.execute('PRAGMA table_info({})'.format(table_id))
  for row in cursor:
    if row[1] == col_id:
      return True
  return False

def column_create(sql, table_id, col_id, col_type='BLOB'):
  if col_exists(sql, table_id, col_id):
    change_column_type(sql, table_id, col_id, col_type)
    return
  try:
    sql.execute('ALTER TABLE {} ADD COLUMN "{}" {}'.format(table_id, col_id, col_type))
  except sqlite3.OperationalError as e:
    if str(e).startswith('duplicate column name'):
      return
    raise e

class Column(object):
  def __init__(self, sql, col):
    self.sql = sql
    self.col = col
    self.col_id = col.col_id
    self.table_id = col.table_id
    create_table(self.sql, self.col.table_id)
    column_create(self.sql, self.col.table_id, self.col.col_id, self.col.type_obj.sql_type())


  def __iter__(self):
    for i in range(0, len(self)):
      if i == 0:
        yield None
      yield self[i]

  def __len__(self):
    len = size(self.sql, self.col.table_id)
    return len + 1
  
  def __setitem__(self, row_id, value):
    if self.col.col_id == 'id':
      if value == 0:
        # print('Deleting by setting id to 0')
        self.__delitem__(row_id)
      return
    column_set(self.sql, self.col.table_id, self.col.col_id, row_id, value)

  def __getitem__(self, key):
    if self.col.col_id == 'id' and key == 0:
      return key
    value = column_raw_get(self.sql, self.col.table_id, self.col.col_id, key)
    return value
  
  def __delitem__(self, row_id):
    # print("Delete [{}][{}]".format(self.col.table_id, row_id))
    self.sql.execute('DELETE FROM {} WHERE id = ?'.format(self.col.table_id), (row_id,))

  def remove(self):
    delete_column(self.sql, self.col.table_id, self.col.col_id)

  def rename(self, new_name):
    rename_column(self.sql, self.table_id, self.col_id, new_name)
    self.col_id = new_name

  def copy_from(self, other):
    if self.col_id == other.col_id and self.table_id == other.table_id:
      return
    try:
      if self.table_id == other.table_id:
        query = ('''
          UPDATE "{}" SET "{}" = "{}"
        '''.format(self.table_id, self.col_id, other.col_id))
        self.sql.execute(query)
        return
      query = ('''
        INSERT INTO "{}" (id, "{}") SELECT id, "{}" FROM "{}" WHERE true
        ON CONFLICT(id) DO UPDATE SET "{}" = excluded."{}"
      '''.format(self.table_id, self.col_id, other.col_id, other.table_id, self.col_id, other.col_id))
      self.sql.execute(query)
    except sqlite3.OperationalError as e:
      if str(e).startswith('no such table'):
        return
      raise e

def column(sql, col):
  return Column(sql, col)

def create_schema(sql):
  sql.executescript('''
    PRAGMA foreign_keys=OFF;
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS "_grist_DocInfo" (id INTEGER PRIMARY KEY, "docId" TEXT DEFAULT '', "peers" TEXT DEFAULT '', "basketId" TEXT DEFAULT '', "schemaVersion" INTEGER DEFAULT 0, "timezone" TEXT DEFAULT '', "documentSettings" TEXT DEFAULT '');
    INSERT INTO _grist_DocInfo VALUES(1,'','','',37,'','');
    CREATE TABLE IF NOT EXISTS "_grist_Tables" (id INTEGER PRIMARY KEY, "tableId" TEXT DEFAULT '', "primaryViewId" INTEGER DEFAULT 0, "summarySourceTable" INTEGER DEFAULT 0, "onDemand" BOOLEAN DEFAULT 0, "rawViewSectionRef" INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_Tables_column" (id INTEGER PRIMARY KEY, "parentId" INTEGER DEFAULT 0, "parentPos" NUMERIC DEFAULT 1e999, "colId" TEXT DEFAULT '', "type" TEXT DEFAULT '', "widgetOptions" TEXT DEFAULT '', "isFormula" BOOLEAN DEFAULT 0, "formula" TEXT DEFAULT '', "label" TEXT DEFAULT '', "description" TEXT DEFAULT '', "untieColIdFromLabel" BOOLEAN DEFAULT 0, "summarySourceCol" INTEGER DEFAULT 0, "displayCol" INTEGER DEFAULT 0, "visibleCol" INTEGER DEFAULT 0, "rules" TEXT DEFAULT NULL, "recalcWhen" INTEGER DEFAULT 0, "recalcDeps" TEXT DEFAULT NULL);
    CREATE TABLE IF NOT EXISTS "_grist_Imports" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "origFileName" TEXT DEFAULT '', "parseFormula" TEXT DEFAULT '', "delimiter" TEXT DEFAULT '', "doublequote" BOOLEAN DEFAULT 0, "escapechar" TEXT DEFAULT '', "quotechar" TEXT DEFAULT '', "skipinitialspace" BOOLEAN DEFAULT 0, "encoding" TEXT DEFAULT '', "hasHeaders" BOOLEAN DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_External_database" (id INTEGER PRIMARY KEY, "host" TEXT DEFAULT '', "port" INTEGER DEFAULT 0, "username" TEXT DEFAULT '', "dialect" TEXT DEFAULT '', "database" TEXT DEFAULT '', "storage" TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS "_grist_External_table" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "databaseRef" INTEGER DEFAULT 0, "tableName" TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS "_grist_TableViews" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "viewRef" INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_TabItems" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "viewRef" INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_TabBar" (id INTEGER PRIMARY KEY, "viewRef" INTEGER DEFAULT 0, "tabPos" NUMERIC DEFAULT 1e999);
    CREATE TABLE IF NOT EXISTS "_grist_Pages" (id INTEGER PRIMARY KEY, "viewRef" INTEGER DEFAULT 0, "indentation" INTEGER DEFAULT 0, "pagePos" NUMERIC DEFAULT 1e999);
    CREATE TABLE IF NOT EXISTS "_grist_Views" (id INTEGER PRIMARY KEY, "name" TEXT DEFAULT '', "type" TEXT DEFAULT '', "layoutSpec" TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS "_grist_Views_section" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "parentId" INTEGER DEFAULT 0, "parentKey" TEXT DEFAULT '', "title" TEXT DEFAULT '', "defaultWidth" INTEGER DEFAULT 0, "borderWidth" INTEGER DEFAULT 0, "theme" TEXT DEFAULT '', "options" TEXT DEFAULT '', "chartType" TEXT DEFAULT '', "layoutSpec" TEXT DEFAULT '', "filterSpec" TEXT DEFAULT '', "sortColRefs" TEXT DEFAULT '', "linkSrcSectionRef" INTEGER DEFAULT 0, "linkSrcColRef" INTEGER DEFAULT 0, "linkTargetColRef" INTEGER DEFAULT 0, "embedId" TEXT DEFAULT '', "rules" TEXT DEFAULT NULL);
    CREATE TABLE IF NOT EXISTS "_grist_Views_section_field" (id INTEGER PRIMARY KEY, "parentId" INTEGER DEFAULT 0, "parentPos" NUMERIC DEFAULT 1e999, "colRef" INTEGER DEFAULT 0, "width" INTEGER DEFAULT 0, "widgetOptions" TEXT DEFAULT '', "displayCol" INTEGER DEFAULT 0, "visibleCol" INTEGER DEFAULT 0, "filter" TEXT DEFAULT '', "rules" TEXT DEFAULT NULL);
    CREATE TABLE IF NOT EXISTS "_grist_Validations" (id INTEGER PRIMARY KEY, "formula" TEXT DEFAULT '', "name" TEXT DEFAULT '', "tableRef" INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_REPL_Hist" (id INTEGER PRIMARY KEY, "code" TEXT DEFAULT '', "outputText" TEXT DEFAULT '', "errorText" TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS "_grist_Attachments" (id INTEGER PRIMARY KEY, "fileIdent" TEXT DEFAULT '', "fileName" TEXT DEFAULT '', "fileType" TEXT DEFAULT '', "fileSize" INTEGER DEFAULT 0, "fileExt" TEXT DEFAULT '', "imageHeight" INTEGER DEFAULT 0, "imageWidth" INTEGER DEFAULT 0, "timeDeleted" DATETIME DEFAULT NULL, "timeUploaded" DATETIME DEFAULT NULL);
    CREATE TABLE IF NOT EXISTS "_grist_Triggers" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "eventTypes" TEXT DEFAULT NULL, "isReadyColRef" INTEGER DEFAULT 0, "actions" TEXT DEFAULT '');
    CREATE TABLE IF NOT EXISTS "_grist_ACLRules" (id INTEGER PRIMARY KEY, "resource" INTEGER DEFAULT 0, "permissions" INTEGER DEFAULT 0, "principals" TEXT DEFAULT '', "aclFormula" TEXT DEFAULT '', "aclColumn" INTEGER DEFAULT 0, "aclFormulaParsed" TEXT DEFAULT '', "permissionsText" TEXT DEFAULT '', "rulePos" NUMERIC DEFAULT 1e999, "userAttributes" TEXT DEFAULT '', "memo" TEXT DEFAULT '');
    INSERT INTO _grist_ACLRules VALUES(1,1,63,'[1]','',0,'','',1e999,'','');
    CREATE TABLE IF NOT EXISTS "_grist_ACLResources" (id INTEGER PRIMARY KEY, "tableId" TEXT DEFAULT '', "colIds" TEXT DEFAULT '');
    INSERT INTO _grist_ACLResources VALUES(1,'','');
    CREATE TABLE IF NOT EXISTS "_grist_ACLPrincipals" (id INTEGER PRIMARY KEY, "type" TEXT DEFAULT '', "userEmail" TEXT DEFAULT '', "userName" TEXT DEFAULT '', "groupName" TEXT DEFAULT '', "instanceId" TEXT DEFAULT '');
    INSERT INTO _grist_ACLPrincipals VALUES(1,'group','','','Owners','');
    INSERT INTO _grist_ACLPrincipals VALUES(2,'group','','','Admins','');
    INSERT INTO _grist_ACLPrincipals VALUES(3,'group','','','Editors','');
    INSERT INTO _grist_ACLPrincipals VALUES(4,'group','','','Viewers','');
    CREATE TABLE IF NOT EXISTS "_grist_ACLMemberships" (id INTEGER PRIMARY KEY, "parent" INTEGER DEFAULT 0, "child" INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_Filters" (id INTEGER PRIMARY KEY, "viewSectionRef" INTEGER DEFAULT 0, "colRef" INTEGER DEFAULT 0, "filter" TEXT DEFAULT '', "pinned" BOOLEAN DEFAULT 0);
    CREATE TABLE IF NOT EXISTS "_grist_Cells" (id INTEGER PRIMARY KEY, "tableRef" INTEGER DEFAULT 0, "colRef" INTEGER DEFAULT 0, "rowId" INTEGER DEFAULT 0, "root" BOOLEAN DEFAULT 0, "parentId" INTEGER DEFAULT 0, "type" INTEGER DEFAULT 0, "content" TEXT DEFAULT '', "userRef" TEXT DEFAULT '');
    CREATE INDEX _grist_Attachments_fileIdent ON _grist_Attachments(fileIdent);

    CREATE TABLE IF NOT EXISTS changes (colRef INTEGER, rowId INTEGER, value BLOB, PRIMARY KEY(colRef, rowId));
    CREATE TABLE IF NOT EXISTS recalc (colRef Integer, rowId INTEGER, seq INTEGER DEFAULT NULL, PRIMARY KEY(colRef, rowId));
    CREATE TABLE IF NOT EXISTS cell_graph (lColumn INTEGER, lRow INTEGER, rColumn INTEGER, rRow INTEGER, PRIMARY KEY(lColumn, lRow, rColumn, rRow));
    CREATE TABLE IF NOT EXISTS filter_graph (lColumn INTEGER, lRow INTEGER, rColumn INTEGER, filter INTEGER, PRIMARY KEY(lColumn, lRow, rColumn, filter));

    CREATE UNIQUE INDEX IF NOT EXISTS "changes_auto" ON "changes" ("colRef", "rowId");
    CREATE UNIQUE INDEX IF NOT EXISTS recalc_auto ON "recalc" ("colRef", "rowId");
    CREATE UNIQUE INDEX IF NOT EXISTS recalc_seq ON "recalc" ("seq", "colRef", "rowId");
    CREATE UNIQUE INDEX IF NOT EXISTS "cell_graph_l" ON "cell_graph" ("lColumn", "lRow", "rColumn", "rRow");
    CREATE INDEX IF NOT EXISTS "colId" ON "_grist_Tables_column" ("colId", "parentId");

    CREATE INDEX IF NOT EXISTS "_grist_Tables_column_parent" ON "_grist_Tables_column" ("parentId", "type");
    CREATE INDEX IF NOT EXISTS "cell_graph_i1" ON "cell_graph" ("lColumn", "lRow", "rColumn");
    CREATE INDEX IF NOT EXISTS "cell_graph_i2" ON "cell_graph" ("rRow", "rColumn", "lColumn", "lRow");
    CREATE INDEX IF NOT EXISTS "cell_graph_i3" ON "cell_graph" ("rColumn", "lRow", "lColumn");
    CREATE INDEX IF NOT EXISTS "changes_value" ON "changes" ("colRef", "value");
    CREATE INDEX IF NOT EXISTS "filter_1" ON "filter_graph" ("lColumn", "lRow", "rColumn", "filter");
    CREATE INDEX IF NOT EXISTS "filter_2" ON "filter_graph" ("rColumn", "filter", "lColumn", "lRow");
    CREATE INDEX IF NOT EXISTS "formulas" ON "_grist_Tables_column" ("parentId", "formula", "isFormula");
    CREATE INDEX IF NOT EXISTS "graph_1" ON cell_graph ("lColumn", "rColumn");
    CREATE INDEX IF NOT EXISTS "graph_2" ON filter_graph ("lColumn", "rColumn");
    CREATE INDEX IF NOT EXISTS "graph_3" ON cell_graph ("rColumn", "rRow");
    CREATE INDEX IF NOT EXISTS "tableId" ON "_grist_Tables" ("tableId");

    COMMIT;
  ''')

def open_connection(file):
  sql = sqlite3.connect(file, isolation_level=None)
  sql.row_factory = sqlite3.Row
  # sql.execute('PRAGMA encoding="UTF-8"')
  # # sql.execute('PRAGMA journal_mode = DELETE;')
  # # sql.execute('PRAGMA journal_mode = WAL;')
  # sql.execute('PRAGMA synchronous = OFF;')
  # sql.execute('PRAGMA trusted_schema = OFF;')
  return sql
