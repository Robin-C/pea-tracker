from datetime import datetime


# Current date time in local system

def loaded_at():
  return datetime.now()


print(loaded_at())