import daqual

import logging
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

print('----test.py----')
x=[
    ['daqual/ExampleCSV.csv',daqual.score_column_count,{"expected_n":8},1,1],
    ['daqual/ExampleCSV.csv',daqual.score_row_count,{"expected_row_count":5},1,1],
    ['daqual/ExampleCSV.csv',daqual.score_column_names,{'columns':['Column 1', 'Second Column', 'Column 3', 'Optional Column 1',
                                            'Optional Column2', 'GroupByColumn','Integer Column', 'Float Column']},1,1],
    ['daqual/ExampleCSV.csv',daqual.score_no_blanks,{'column':'Second Column'},1,1],
    ['daqual/ExampleCSV.csv',daqual.score_float,{'column':'Float Column'},1,1],
    ['daqual/groups.csv',daqual.score_column_count,{"expected_n":1},1,1],
    ['daqual/ExampleCSV.csv',daqual.score_column_valid_values,{'column':'GroupByColumn', 'master':'daqual/groups.csv',
                                                    'master_column':'groups'},1,1],
    ['daqual/ExampleCSV.csv', daqual.score_unique_column, {'column': 'GroupByColumn'}, 1, 1],

    ['daqual/groups.csv',daqual.score_unique_column,{'column':'groups'},1,1],
    ['daqual/groups.csv',daqual.score_row_count,{'expected_row_count':3},1,1],
    ['daqual/groups.csv',daqual.score_match,{'column':'groups', 'match':'Group\d'},1,1],

    ['daqual/ExampleCSV-2.csv',daqual.score_column_count,{"expected_n":9},1,1],
    ['daqual/ExampleCSV-2.csv',daqual.score_row_count,{"comparison":'daqual/ExampleCSV.csv', 'expected_delta':1},1,1],
    ['daqual/ExampleCSV-2.csv',daqual.score_comparison,{"comparison":'daqual/ExampleCSV.csv',
                                                        'delta':0, 'comparator':'<=','column':"Float Column"},1,1],
]

#d = daqual.Daqual(daqual.Daqual.aws_provider)
d = daqual.Daqual(daqual.Daqual.file_system_provider)
q, vlist = d.validate_objects(x)

logger.info('*** Total average quality measure: {} ***'.format(q))
#logger.info('*** Validation list: {} ***'.format(vlist))
