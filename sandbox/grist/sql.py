import json
import marshal
import sqlite3
import actions
import six


import sqlite3

def change_column_type(conn, table_name, column_name, new_type):
  cursor = conn.cursor()
  try:
    cursor.execute('PRAGMA table_info("{}");'.format(table_name))
    columns_info = cursor.fetchall()
    old_type = new_type
    for col in columns_info:
      if col[1] == column_name:
        old_type = col[2].upper()
        break
    if old_type == new_type:
      return
    new_columns_def = ", ".join(
      '"{}" {}{}'.format(col[1], new_type if col[1] == column_name else col[2], " DEFAULT " + str(col[4]) if col[4] is not None else "")
      for col in columns_info
    )
    
    column_names = ", ".join(quote(col[1]) for col in columns_info)
    cursor.execute('CREATE TABLE "new_{}" ({});'.format(table_name, new_columns_def))
    cursor.execute('INSERT INTO "new_{}" SELECT {} FROM "{}";'.format(table_name, column_names, table_name))
    cursor.execute('DROP TABLE "{}";'.format(table_name))
    cursor.execute('ALTER TABLE "new_{}" RENAME TO "{}";'.format(table_name, table_name))
  finally:
    cursor.close()



def is_primitive(value):
  string_types = six.string_types if six.PY3 else (str,)
  numeric_types = six.integer_types + (float,)
  bool_type = (bool,)
  return isinstance(value, string_types + numeric_types + bool_type)

def quote(name):
  return '"{}"'.format(name)

def delete_column(conn, table_name, column_name):
  cursor = conn.cursor()
  try:
    cursor.execute('PRAGMA table_info("{}");'.format(table_name))
    columns_info = cursor.fetchall()
    
    new_columns_def = ", ".join(
      '"{}" {}{}'.format(col[1], col[2], " DEFAULT " + str(col[4]) if col[4] is not None else "")
      for col in columns_info
      if col[1] != column_name
    )
    
    column_names = ", ".join(quote(col[1]) for col in columns_info if col[1] != column_name)
    
    if new_columns_def:
      cursor.execute('CREATE TABLE "new_{}" ({})'.format(table_name, new_columns_def))
      cursor.execute('INSERT INTO "new_{}" SELECT {} FROM "{}"'.format(table_name, column_names, table_name))
      cursor.execute('DROP TABLE "{}"'.format(table_name))
      cursor.execute('ALTER TABLE "new_{}" RENAME TO "{}"'.format(table_name, table_name))
  finally:
    cursor.close()





def is_primitive(value):
  string_types = six.string_types if six.PY3 else (str,)
  numeric_types = six.integer_types + (float,)
  bool_type = (bool,)
  return isinstance(value, string_types + numeric_types + bool_type)


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
  sql.execute('PRAGMA encoding="UTF-8"')
  # # sql.execute('PRAGMA journal_mode = DELETE;')
  # sql.execute('PRAGMA journal_mode = WAL;')
  # sql.execute('PRAGMA synchronous = OFF;')
  # sql.execute('PRAGMA trusted_schema = OFF;')
  return sql
