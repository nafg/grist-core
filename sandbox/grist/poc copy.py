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
  apply(['AddRawTable', 'Types' ])
  apply(['AddColumn', 'Types', 'numeric', {'type': 'Numeric'}])
  apply(['AddRecord', 'Types', None, {'numeric': False}])
finally:
  if hasattr(eng, 'close'):
    eng.close()
 


# try:
#   apply(['AddRawTable', 'Types' ])
#   apply(['AddColumn', 'Types', 'text', {'type': 'Text'}])
#   apply(['AddRecord', 'Types', None, {'text': None}])
#   w = (apply(["ModifyColumn", "Types", "text", { "type" : "Bool" }]))
#   print(w)
# finally:
#   if hasattr(eng, 'close'):
#     eng.close()