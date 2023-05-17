import engine
import useractions


eng = engine.Engine()
eng.load_empty()


def apply(actions):
  if not actions:
    return []
  if not isinstance(actions[0], list):
    actions = [actions]
  return eng.apply_user_actions([useractions.from_repr(a) for a in actions])
  

try:
  # Ref column
  def ref_columns():
    apply(['AddRawTable', 'Table1'])
    apply(['AddRawTable', 'Table2'])
    apply(['AddRecord', 'Table1', None, {"A": 30}])
    apply(['AddColumn', 'Table2', 'R', {'type': 'Ref:Table1'}]),
    apply(['AddColumn', 'Table2', 'F', {'type': 'Any', "isFormula": True, "formula": "$R.A"}]),
    apply(['AddRecord', 'Table2', None, {'R': 1}])
    apply(['UpdateRecord', 'Table1', 1, {'A': 40}])
    print(eng.fetch_table('Table2'))


  # Any lookups
  def any_columns():
    apply(['AddRawTable', 'Table1'])
    apply(['AddRawTable', 'Table2'])
    apply(['AddRecord', 'Table1', None, {"A": 30}])
    apply(['AddColumn', 'Table2', 'R', {'type': 'Any', 'isFormula': True, 'formula': 'Table1.lookupOne(id=1)'}]),
    apply(['AddColumn', 'Table2', 'F', {'type': 'Any', "isFormula": True, "formula": "$R.A"}]),
    apply(['AddRecord', 'Table2', None, {}])
    print(eng.fetch_table('Table2'))
    # Change A to 40
    apply(['UpdateRecord', 'Table1', 1, {'A': 40}])
    print(eng.fetch_table('Table2'))

  # Any lookups
  def simple_formula():
    apply(['AddRawTable', 'Table1'])
    apply(['ModifyColumn', 'Table1', 'B', {'type': 'Numeric', 'isFormula': True, 'formula': 'Table1.lookupOne(id=$id).A + 10'}]),
    apply(['AddRecord', 'Table1', None, {"A": 1}])
    print(eng.fetch_table('Table1'))

    apply(['UpdateRecord', 'Table1', 1, {"A": 2}])
    print(eng.fetch_table('Table1'))

    apply(['UpdateRecord', 'Table1', 1, {"A": 3}])
    print(eng.fetch_table('Table1'))

  simple_formula()

finally:
  # Test if method close is in engine
  if hasattr(eng, 'close'):
    eng.close()
 