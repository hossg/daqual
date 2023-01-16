import logging
import pandas as pd
import subprocess
import numpy as np

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class daqual2:

    def __init__(self, filename):
        self.df = pd.read_csv(filename)
        self.filename = filename
        self.invalid_rows = []



        process = subprocess.Popen(['shasum', filename],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        self.hash = stdout.split()[0].decode('UTF-8')



    def score(self, param):
        field_score = self.score_column_names(list(param['fields'].keys()))
        row_score = self.score_row_count(param['expected_rows'])
        return (field_score,row_score,3)


    def score_row_count(self, expected_rows=-1):
        if expected_rows==-1:
            return 1                # if we have no expectation re: number of rows, then any number is 100% success
        row_count = len(self.df)
        difference = abs(row_count-expected_rows)
        score = 1 - difference / expected_rows
        if score < 0:
            score = 0
        return score
    # TODO - need to put logging in here to record reasons for imperfect scoring

    def score_column_names(self, column_names):
        if (self.df.columns.to_list() == column_names):
            return 1
        else:
            return 0
    # TODO - need to put logging in here to record reasons for imperfect scoring

    # def score_field_match(self):
    #     for row in self.df.iterrows():
    #         if



    def score_field_int(self, column_name):
        dt = self.df[column_name].dtype
        if dt == np.dtype('int64'):
            return 1  # if every row is an int then return 1, but otherwise, let's go and find the rows that are not
        else:
            valid=0
            invalid=0
            for n,item in enumerate( self.df[column_name]):
                if str(item).isdigit():
                    valid += 1
                else:
                    invalid += 1
                    print(f'row {n} column {column_name} is not an integer')
            return valid/(valid+invalid)

    # for a provided function see get a list of the row indices that do not meet the criteria of the provided function
    def get_invalid_rows(self, function, column):
        return list(self.df[self.df.applymap(function)[column] == False].index)

    # for a provided function get the % of rows that do not match that function for a given column
    # store the indices of the failing rows in the object
    def generic_score(self,function, column):
        invalid_rows = self.get_invalid_rows(function,column)
        total_rows=len(self.df[column])
        row_score = (total_rows-len(invalid_rows))/total_rows
        self.invalid_rows[column] += invalid_rows
        return row_score


    def calculate_column_score(self):
        invalid_columns = len(self.invalid_rows)
        total_columns = len(self.df.columns)
        column_score = (total_columns-invalid_columns/total_columns)
        return column_score

    def calculate_row_score(self):
        unique_invalid_rows = set()
        for column in self.invalid_rows:
            unique_invalid_rows.add(column)
        total_invalid_rows = len(unique_invalid_rows)
        total_rows=len(self.df)
        row_score = (total_rows-total_invalid_rows)/total_rows
        return row_score



    # def score(self,params):
    #     expected_columns = params['columns'].keys()
    #     for c in expected_columns:
    #         if c in self.df.columns:
    #             column_count += 1
    #
    #     field_score = column_count / len(expected_columns)
    #
    #
    #     return (field_score, record_score, attribute_score)

# print(os.getcwd())
# dq = daqual2('../examples/daqual/iso-currencies.csv')



def test_score():
    filename = '../examples/daqual/iso-currencies.csv'
    d=daqual2(filename)
    param = {"fields": {"ENTITY":1,"Currency":1,"Alphabetic Code":1,"Numeric Code":1,"Minor unit":1},"expected_rows":280}
    print(d.score(param))



if __name__=='__main__':
    test_score()

