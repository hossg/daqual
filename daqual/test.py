import daqual

import logging
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


# Some business quality rules to implement:
# Need to think about separation between syntactic quality and semantic quality
# Latter implies business rules more?

# 0.

# 1. Accounts file -
# Do I have more accounts than yesterday?
# Do all accounts have the same currencies as yesterday?
# Do all accounts have currencies?
# Are all currencies in the ISO codes list?
# Do all accounts meet the agreed/defined naming format?
# Are all accounts unqiue?

# 2. Balances file -
# Does every account correspond to a valid account?
# Does every account exist in the file?
# Does an account exist for every balance?
# Are all accounts unique
# Does a closing balance exist for every account?
# Are all balances greater than zero?

# 3. Transactions file -
# Does every transaction have an amount?
# Do all transactions have a valid account?
# Are all amounts numbers?
# Does the opening balance plus the transaction amount equal the closing balance?
# Is the transaction ID unique
# Are all transaction dates consistent with file date?

# File name corresponds to file contents?

# Object handling ideas:
# get yesterday's file
# get last file
# get last business date file





# a function to look through String objects in the validation list and pre-process them according
# to str.format() rules
def custom_transformer(validation_list, kwargs):
    output_list = []

    for item in validation_list:
        output_jitems = []
        for jitem in item:
            if type(jitem) == str:
                replacement_jitem = jitem.format(**kwargs)
                output_jitems.append(replacement_jitem)
            elif type(jitem) == dict:
                for k in jitem.keys():
                    if k != 'match' and type(jitem.get(k)) == str:
                        jitem[k] = jitem.get(k).format(**kwargs)
                output_jitems.append(jitem)
            else:
                output_jitems.append(jitem)
        output_list.append(output_jitems)
    return output_list

# Worked Example

# Currencies list - using the ISO4217 currencies file downloaded from http://iso.org
# Expecting 5 columns
# Expecting 279 rows
# Expecting specific column names
# Do all currencies meet the ISO format - e.g. 3 chrs or blank (some territories do not have a currency) ?
currencies = [
    ['daqual/iso-currencies.csv',daqual.score_column_count,{"expected_columns": 5},1,1],
    ['daqual/iso-currencies.csv',daqual.score_row_count,{"expected_rows":279},1,1],
    ['daqual/iso-currencies.csv',daqual.score_column_names,{"columns":['ENTITY','Currency','Alphabetic Code',
                                                                       'Numeric Code','Minor unit']},1,1],
    ['daqual/iso-currencies.csv',daqual.score_column_format,{"column": "Alphabetic Code",'match': '^$|[A-Z]{3}'},1,1]
]

# accounts = [
#     ['daqual/accounts-{previous_business_date}.csv', daqual.score_1, {}, 1, 1],
#     # basic validation of yesterday's file, since we will depend upon this later for comparison purposes
#     ['daqual/accounts-{business_date}.csv', daqual.score_column_count, {"expected_n": 2}, 1, 1],  # expecting 2 columns
#     ['daqual/accounts-{business_date}.csv', daqual.score_unique_column, {"column": "Account Number"}, 1, 1],
#     # account number should be unique
#     ['daqual/accounts-{business_date}.csv', daqual.score_row_count,
#      {"comparison": 'daqual/accounts-{previous_business_date}.csv',
#       "expected_delta": ">="}, 1, 1],
#     # expecting at least as many rows as the previous day, since accounts can only be created
#     ['daqual/accounts-{business_date}.csv', daqual.score_match, {"column": "Account Number",
#                                                                  'match': '[a-zA-Z]-[\d]{3}'}, 1, 1],
#     # account number should have the format letter-3-digits
#     ['daqual/accounts-{business_date}.csv', daqual.score_match, {"column": "Currency", 'match': '[A-Z]{3}'}, 1, 1]
#     # currency should have the format 3 capital letters
# ]
#
# balances = [
#     ['daqual/accounts-{business_date}.csv', daqual.score_1, {}, 1, 1],  # basic validation for a file we'll need shortly
#
#     ['daqual/balances-{business_date}.csv', daqual.score_column_names, {"columns": ['account', 'balance']}, 1, 1],
#     # expecting two columns
#     ['daqual/balances-{business_date}.csv', daqual.score_column_valid_values,
#      # do all accounts appear in the master list?
#      {'column': 'account', 'master': 'daqual/accounts-{business_date}.csv', 'master_column': 'Account Number'}, 1, 1],
#     ['daqual/balances-{business_date}.csv', daqual.score_every_master_value_used,  # does every account have a balance?
#      {'column': 'account', 'master': 'daqual/accounts-{business_date}.csv', 'master_column': 'Account Number'}, 1, 1],
# ]
#
# transactions = [
#     ['daqual/accounts-{business_date}.csv', daqual.score_1, {}, 1, 1],  # basic validation for a file we'll need shortly
#     ['daqual/transactions-{previous_business_date}.csv', daqual.score_1, {}, 1, 1],  # basic validation for a file we'll need shortly
#     ['daqual/transactions-{business_date}.csv', daqual.score_column_valid_values,
#      # do all accounts appear in the master list?
#      {'column': 'account', 'master': 'daqual/accounts-{business_date}.csv', 'master_column': 'Account Number'}, 1, 1],
#     ['daqual/transactions-{business_date}.csv', daqual.score_number, {"column": "amount"}, 1, 1],
#     # the amount must be a number!
#     ['daqual/transactions-{business_date}.csv', daqual.score_no_blanks, {"column": "amount"}, 1, 1],
#     # and amount can't be blank
#     ['daqual/transactions-{business_date}.csv', daqual.score_date, {"column": "date"}, 1, 1],
#
#     ['daqual/transactions-{business_date}.csv', daqual.score_comparison,  # does every account have a balance?
#      {'column': 'value', 'comparison': 'daqual/transactions-{previous_business_date}.csv',
#       "groupby":{"columns":['account','credit/debit'],"aggregation":"sum"}, "comparator":"between",
#       "factor":1.2}, 1, 1]
# ]


# for this example, let's just pick a specific business date, and calculate the previous business date from that
business_date = "20190304"
iso_business_date = '-'.join(
    [business_date[0:4], business_date[4:6], business_date[6:8]])  # we need this for get_prev/next_business_date
date_config = {
    "business_date": business_date,
    "previous_business_date": daqual.Daqual.get_prev_date(iso_business_date, daqual.Daqual.default_calendar,
                                                          isoabbreviated=True)
}

# TODO - need to consider what to do if/when we want to add more tests to the same validator - cumulative scores, etc?
# Consider whether we can re-use objects that have already been validated. Should be able to.
a = daqual.Daqual(daqual.Daqual.file_system_provider)

quality, validation_results = a.validate_objects(custom_transformer(currencies, date_config))
logger.info('*** Accounts: total average quality measure: {} ***'.format(quality))

# quality, validation_results = a.validate_objects(custom_transformer(accounts, date_config))
# logger.info('*** Accounts: total average quality measure: {} ***'.format(quality))
#
# quality, validation_results = a.validate_objects(custom_transformer(balances, date_config))
# logger.info('*** Balances: total average quality measure: {} ***'.format(quality))
#
# quality, validation_results = a.validate_objects(custom_transformer(transactions, date_config))
# logger.info('*** Transactions: total average quality measure: {} ***'.format(quality))
