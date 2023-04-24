import difflib
import functools
import json
import unittest
from collections import namedtuple
from pprint import pprint

import six

import actions
import column
import engine
import logger
import useractions
import testutil
import objtypes


eng = engine.Engine()
eng.load_empty()


def apply(actions):
  if not actions:
    return []
  if not isinstance(actions[0], list):
    actions = [actions]
  return eng.apply_user_actions([useractions.from_repr(a) for a in actions])
  

try:
  apply(['AddRawTable', 'Table1'])
  apply(['AddRecord', 'Table1', None, {'A': 1, 'B': 2, 'C': 3}])
  apply(['AddColumn', 'Table1', 'D', {'type': 'Numeric', 'isFormula': True, 'formula': '$A + 3'}]),
  apply(['RenameColumn', 'Table1', 'A', 'NewA'])
  apply(['RenameTable', 'Table1', 'Dwa'])
  apply(['RemoveColumn', 'Dwa', 'B'])
  apply(['RemoveTable', 'Dwa'])

  #  ['RemoveColumn', "Table1", 'A'],
    # ['AddColumn', 'Table1', 'D', {'type': 'Numeric', 'isFormula': True, 'formula': '$A + 3'}],
    # ['ModifyColumn', 'Table1', 'B', {'type': 'Numeric', 'isFormula': True, 'formula': '$A + 1'}],
  #])

    # ['AddColumn', 'Table1', 'D', {'type': 'Numeric', 'isFormula': True, 'formula': '$A + 3'}],
    # ['ModifyColumn', 'Table1', 'B', {'type': 'Numeric', 'isFormula': True, 'formula': '$A + 1'}],
finally:
  # Test if method close is in engine
  if hasattr(eng, 'close'):
    eng.close()
 