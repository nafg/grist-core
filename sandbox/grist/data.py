class ColumnData(object):
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