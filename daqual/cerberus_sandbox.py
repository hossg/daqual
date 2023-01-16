from cerberus import *
import pandas as pd

schema = {'name': {'type': 'string'}, 'age': {'type': 'integer', 'min': 10}}
V = Validator(schema)

document = {'name': 12, 'age': 5}
print(V.validate(document))
print(V.errors)
print(len(V.errors))


df=pd.read_csv('examples/daqual/transactions-20190301.csv')
print(df.to_dict(orient="records"))
print(df.to_dict())
df.set_index('account',inplace=True)
print(df.head())
print(df.to_dict(orient="records"))
print(df.to_dict())



# if there's an index, use that as the dict key - needs to be a UNIQUE index
# if not, use row1, row2, etc, as dict keys
# if there's a header row, use this as column names
# if some rows have more cols than in the header row, additional columns are named 'columnN'


# validate a row at a time