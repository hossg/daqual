import pandas as pd
d = pd.read_csv('c:\\users\\ghodseh\\Desktop\\ExampleCSV.csv')
master = pd.read_csv('c:\\users\\ghodseh\\Desktop\\groups.csv')

print(d)

def score_column_count(dataframe, expected_n):
    score=len(dataframe.columns)/expected_n
    if (score > 1):
        score = 0
    return score

print("score_column_count(d,8): {}".format(score_column_count(d,8)))
print("score_column_count(d,7): {}".format(score_column_count(d,7)))
print("score_column_count(d,10): {}".format(score_column_count(d,10)))

def score_column_names(df, columnnames):
    if (df.columns.to_list() == columnnames):
        return 1
    else:
        return 0

print("score_column_names(d,['Column 1', 'Second Column', 'Column 3', 'Optional Column 1',\
       'Optional Column2', 'GrouoByColumn', 'Integer Column', 'Float Column']): {}".format(score_column_names(d,\
       ['Column 1', 'Second Column', 'Column 3', 'Optional Column 1','Optional Column2', 'GrouoByColumn',\
        'Integer Column', 'Float Column'])))
print("score_column_names(d,['Column 1', 'Second Column', 'Column 3', 'Optional Column 1',\
       'Optional Column2', 'GroupByColumn', 'Integer Column', 'Float Column']): {}".format(score_column_names(d,\
       ['Column 1', 'Second Column', 'Column 3', 'Optional Column 1','Optional Column2', 'GroupByColumn',\
        'Integer Column', 'Float Column'])))


def score_no_blanks(df, column):
    if column in df.columns[df.isna().any()].tolist():
        return 0
    else:
        return 1

print('score_no_blanks(d,"Second Column"): {}'.format(score_no_blanks(d,"Second Column")))
print('score_no_blanks(d,"Column 1"): {}'.format(score_no_blanks(d,"Column 1")))


# provide a master data frame, and column therein, and confirm the % of values from our regular dataframe, and column,
# that are present in that master set. useful check of referential integrity and consistency across files
def score_column_valid_values(df, column, master_df, master_column):
    s=df[column].to_list()
    m=master_df[master_column].to_list()
    print(s)
    print(m)
    c=0
    for i in s:
        if i in m:
            c+=1
    return c/len(s)
    print(type(s))
    print(type(m))

print("validity score: {}".format(score_column_valid_values(d,'GroupByColumn',master, 'groups')))


x=[[score_column_count,(8),1,1],\
    [score_column_count,(8),1,1],\
    [score_column_count,(8),1,1],\
    [score_column_names,['Column 1', 'Second Column', 'Column 3', 'Optional Column 1','Optional Column2', 'GrouoByColumn',\
        'Integer Column', 'Float Column'],1,1],\
    [score_no_blanks,'Second Column',1,1]]

quality=0.0
for item in x:
    si = item[0](d,item[1])
    print(si)
    thresholdi = item[3]
    if(si<thresholdi):
        print("threshold failure: individual item of low quality")
    quality+=(item[2]*si)
print(quality)
quality = quality/len(x)

print(quality)
