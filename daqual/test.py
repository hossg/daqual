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

# #d = daqual.Daqual(daqual.Daqual.aws_provider)
# d = daqual.Daqual(daqual.Daqual.file_system_provider)
# q, vlist = d.validate_objects(x)
#
# logger.info('*** Total average quality measure: {} ***'.format(q))
# #logger.info('*** Validation list: {} ***'.format(vlist))


# Worked Example

# First let's check the accounts file

a = daqual.Daqual(daqual.Daqual.file_system_provider)
accounts = [
    ['daqual/accounts-20190301.csv',daqual.score_1,{},1,1],                                             # basic validation of yesterday's file, since we will depend upon this later for comparison purposes

    ['daqual/accounts-20190304.csv',daqual.score_column_count,{"expected_n":2},1,1],                    # expecting 2 columns
    ['daqual/accounts-20190304.csv',daqual.score_unique_column,{"column":"Account Number"},1,1],        # account number should be unique
    ['daqual/accounts-20190304.csv',daqual.score_row_count,{"comparison":'daqual/accounts-20190301.csv',
                                                            "expected_delta":">="},1,1] ,               # expecting at least as many rows as the previous day, since accounts can only be created
    ['daqual/accounts-20190304.csv',daqual.score_match,{"column":"Account Number",
                                                        'match':'[a-zA-Z]-[\d]{3}'},1,1],               # account number should have the format letter-3-digits
    ['daqual/accounts-20190304.csv',daqual.score_match,{"column":"Currency", 'match':'[A-Z]{3}'},1,1]   # currency should have the format 3 capital letters
]

balances = [
    ['daqual/accounts-20190304.csv',daqual.score_1,{},1,1],                                             # basic validation for a file we'll need shortly

    ['daqual/balances-20190304.csv',daqual.score_column_names,{"columns":['account','balance']},1,1],   # expecting two columns
    ['daqual/balances-20190304.csv',daqual.score_column_valid_values,                                   # do all accounts appear in the master list?
        {'column':'account', 'master':'daqual/accounts-20190304.csv','master_column':'Account Number'},1,1],
    ['daqual/balances-20190304.csv',daqual.score_every_master_value_used,                               # does every account have a balance?
        {'column':'account', 'master':'daqual/accounts-20190304.csv','master_column':'Account Number'},1,1]

]

transactions = [
    ['daqual/accounts-20190304.csv',daqual.score_1,{},1,1],                                             # basic validation for a file we'll need shortly
    ['daqual/transactions-20190304.csv',daqual.score_column_valid_values,                                   # do all accounts appear in the master list?
        {'column':'account', 'master':'daqual/accounts-20190304.csv','master_column':'Account Number'},1,1],
    ['daqual/transactions-20190304.csv', daqual.score_number, {"column": "amount"},1,1],                # the amount must be a number!
    ['daqual/transactions-20190304.csv', daqual.score_no_blanks, {"column": "amount"},1,1],             # and amount can't be blank
    ['daqual/transactions-20190304.csv', daqual.score_date, {"column": "date"},1,1],
]

# TODO - need to consider what to do if/when we want to add more tests to the same validator - cumulative scores, etc?
# Consider whether we can re-use objects that have already been validated. Should be able to.
quality, validation_results = a.validate_objects(accounts)
logger.info('*** Accounts: total average quality measure: {} ***'.format(quality))

quality, validation_results = a.validate_objects(balances)
logger.info('*** Balances: total average quality measure: {} ***'.format(quality))

quality, validation_results = a.validate_objects(transactions)
logger.info('*** Transactions: total average quality measure: {} ***'.format(quality))

def custom_transformer(validation_list,kwargs):
    output_list=[]

    for item in validation_list:
        output_jitems=[]
        for jitem in item:
            if type(jitem)==str:
                replacement_jitem=jitem.format(**kwargs)
                output_jitems.append(replacement_jitem)
            elif type(jitem)==dict:
                for k in jitem.keys():
                    if k !='match' and type(jitem.get(k))==str:
                        jitem[k]=jitem.get(k).format(**kwargs)
                output_jitems.append(jitem)
            else:
                output_jitems.append(jitem)
        output_list.append(output_jitems)
    return output_list


transactions = [
    ['daqual/accounts-{0}.csv',daqual.score_1,{},1,1],                                             # basic validation for a file we'll need shortly
    ['daqual/transactions-{0}.csv',daqual.score_column_valid_values,                               # do all accounts appear in the master list?
        {'column':'account', 'master':'daqual/accounts-{0}.csv','master_column':'Account Number'},1,1],
    ['daqual/transactions-{0}.csv', daqual.score_number, {"column": "amount"},1,1],                # the amount must be a number!
    ['daqual/transactions-{0}.csv', daqual.score_no_blanks, {"column": "amount"},1,1],             # and amount can't be blank
    ['daqual/transactions-{0}.csv', daqual.score_date, {"column": "date"},1,1],
]




# quality, validation_results = a.validate_objects(custom_transformer(transactions,business_date))
# logger.info('*** Transactions: total average quality measure: {} ***'.format(quality))

accounts = [
    ['daqual/accounts-{previous_business_date}.csv',daqual.score_1,{},1,1],                                             # basic validation of yesterday's file, since we will depend upon this later for comparison purposes

    ['daqual/accounts-{business_date}.csv',daqual.score_column_count,{"expected_n":2},1,1],                    # expecting 2 columns
    ['daqual/accounts-{business_date}.csv',daqual.score_unique_column,{"column":"Account Number"},1,1],        # account number should be unique
    ['daqual/accounts-{business_date}.csv',daqual.score_row_count,{"comparison":'daqual/accounts-{previous_business_date}.csv',
                                                            "expected_delta":">="},1,1] ,               # expecting at least as many rows as the previous day, since accounts can only be created
    ['daqual/accounts-{business_date}.csv',daqual.score_match,{"column":"Account Number",
                                                        'match':'[a-zA-Z]-[\d]{3}'},1,1],               # account number should have the format letter-3-digits
    ['daqual/accounts-{business_date}.csv',daqual.score_match,{"column":"Currency", 'match':'[A-Z]{3}'},1,1]   # currency should have the format 3 capital letters
]

business_date="20190304"
iso_business_date='-'.join([business_date[0:4],business_date[4:6],business_date[6:8]])
date_config={
    "business_date": business_date,
    "previous_business_date": daqual.Daqual.get_prev_date(iso_business_date,daqual.Daqual.custom_calendar,isoabbreviated=True)
}


print(date_config)
quality, validation_results = a.validate_objects(custom_transformer(accounts,date_config))
logger.info('*** Transactions: total average quality measure: {} ***'.format(quality))