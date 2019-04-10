import pandas as pd
import numpy as np
import re
import logging
import boto3
import botocore
import shutil
import pathlib

file_system_provider_root = '../examples/'
temp_folder = './temp/'

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = 'daqual'  # replace with your bucket name
s3 = boto3.resource('s3')

class Daqual:

    global score_row_count
    global score_column_names
    global score_no_blanks
    global score_column_valid_values
    global score_every_master_value_used
    global score_unique_column
    global score_number
    global score_float
    global score_int
    global score_match
    global score_column_count


    def __init__(self, provider):
        self.object_list = {}
        self.provider=provider


    def retrieve_object_from_filesystem(objectkey):

        bucket, file_objectkey = objectkey.split('/',1)
        pathlib.Path(temp_folder+bucket).mkdir(parents=True, exist_ok=True)
        tempfilename=temp_folder + file_objectkey

        filename = file_system_provider_root + objectkey
        shutil.copyfile(filename,tempfilename)
        df = pd.read_csv(tempfilename)
        # TODO - tidy up/remove tempfile when the instance is destroyed

        logger.info("Retrieved and converted object {}".format(filename))
        return (1,df)


    # retrieve objects from S3 and return a pandas DataFrame
    def retrieve_object_from_S3(objectkey):
        # TODO - switch from using /temp to perhaps /temp and then a unique-per-instance identifier
        bucket, s3_objectkey = objectkey.split('/',1)
        pathlib.Path(temp_folder+bucket).mkdir(parents=True, exist_ok=True)
        tempfilename=temp_folder + objectkey
        try:
            s3.Bucket(bucket).download_file(s3_objectkey, tempfilename)
        except botocore.exceptions.ClientError as e:
            logger.error("Could not retrieve object {}".format(objectkey))
            return(0, None)
        df = pd.read_csv(tempfilename)
        # TODO - tidy up/remove tempfile when the instance is destroyed

        logger.info("Retrieved and converted object {}".format(objectkey))
        return (1,df)

    def update_object_tagging(self,objectkey, tag, value):

        bucket, s3_objectkey = objectkey.split('/', 1)
        tags = boto3.client('s3').get_object_tagging(Bucket=bucket,Key=s3_objectkey)
        t = tags['TagSet']
        if len(t) >0:
            for i in t:
                if i['Key']==tag:
                    i['Value']=str(value)
        else:
            t.append({'Key': tag, 'Value': str(value)})

        ts = {'TagSet':t}
        boto3.client('s3').put_object_tagging(Bucket=bucket, Key=s3_objectkey, Tagging=ts)


    def set_quality_score(self,objectkey, quality):
        logger.info("Setting quality_score on {} to {}".format(objectkey,quality))
        self.update_object_tagging(objectkey,'quality_score', quality)



    def get_dataframe(self,object_name):
        return self.object_list[object_name]['dataframe']

    # each item in the object list is thus:
    # [object_name, check_function, function parameter, weight, threshold]
    def validate_objects(self,validation_list):

        # first retrieve all required objects, create dataframes for them
        # TODO - need to rename the various item[] references to make the code more intelligible
        for item in validation_list:
            if item[0] not in self.object_list.keys():    # if we haven't already retrieved and processed this object
                #score,df = self.retrieve_object_from_S3(item[0])
                score, df = self.provider['retrieve'](item[0])
                setattr(df,'objectname',item[0])
                if score == 1:
                    self.object_list[item[0]]={} # create the key and the dict
                    self.object_list[item[0]]['dataframe']=df
                    self.object_list[item[0]]['quality'] = 0
                    self.object_list[item[0]]['n_tests'] = 0
                    self.object_list[item[0]]['total_weighting'] = 0
                if score == 0:
                    return 0            # each array is defined such that ALL files must exist


        # if we have all required files, then for each entry in the list, we perform the requisite test
        # and for each individual file we keep track of the cumulative quality score for that file
        for item in validation_list:

            individual_threshold = item[4]
            individual_weight = item[3]
            self.object_list[item[0]]['n_tests'] += 1
            self.object_list[item[0]]['total_weighting'] += individual_weight

            individual_test_score = item[1](self,item[0], item[2])
            logger.info('Validating {} with test {}({}) - Quality Score = {}'.format(item[0], item[1].__name__,
                                                                                     item[2], individual_test_score))

            individual_threshold = item[4]
            individual_weight = item[3]
            if (individual_test_score < individual_threshold):
                logger.warn("Threshold failure: {} {}({}) scored {}, expecting at least {}".format(
                    item[0],item[1].__name__,item[2],individual_test_score,individual_threshold))
                # TODO - need to actually fail the validation if a threshold is failed
            self.object_list[item[0]]['quality'] += (individual_weight * individual_test_score)

        average_quality = 0
        for i in self.object_list.keys():
            self.object_list[i]['quality'] /= self.object_list[i]['total_weighting']
            self.set_quality_score(i, self.object_list[i]['quality'])
            average_quality += self.object_list[i]['quality']
            logger.info("Object summary for {} - quality: {}, n_tests: {}".format(i, self.object_list[i]['quality'], self.object_list[i]['n_tests']))
        average_quality /= len(self.object_list)

        return average_quality, self.object_list # return also the actual list of dataframes, files, etc. Necessary?

    # get the % of columns expected; if you get too many columns, it still returns a % showing your overage, upto a maximum
    # if you have double or more the number of colums desired, the returned score is 0
    #
    # param: expected_n - the number of expected columns
    def score_column_count(self, object_name, p):
        dataframe = self.get_dataframe(object_name)
        score=len(dataframe.columns)/p['expected_n']
        if score <= 1:
            return score
        if (score > 2):
            score = 0
        elif (score > 1):
            score = 2 - score

        logger.warn("Object {} has {} columns, expecting only {}".format(dataframe.objectname,len(dataframe.columns),p['expected_n']))
        return score

    # is a mandatory column complete?
    #
    # param: column name to check
    def score_no_blanks(self,object_name, p):
        df=self.get_dataframe(object_name)
        if p['column'] in df.columns[df.isna().any()].tolist():
            return 0
        else:
            return 1

    # are the column names as expected?
    #
    # param: columns - a list of expected columns
    def score_column_names(self,object_name, p):
        df=self.get_dataframe(object_name)
        if (df.columns.to_list() == p['columns']):
            return 1
        else:
            return 0

    # provide a master data frame, and column therein, and confirm the % of values from our regular dataframe, and column,
    # that are present in that master set. useful check of referential integrity and consistency across files
    #
    # params: column, master, master_column
    def score_column_valid_values(self,object_name,p):
        df=self.get_dataframe(object_name)

        master = self.get_dataframe(p['master'])
        s=df[p['column']].to_list()

        m=master[p['master_column']].to_list()
        c=0
        for i in s:
            if i in m:
                c+=1
        if c < len(s):
            logger.warn('Unexpected values in {} column {}; values are not in master data {} col'.format(
                df['objectname'],p['column'],p['master'],p['master_column']
            ))
        return c/len(s)

    # provide a master data frame, and column therein, and confirm that each and every value from that master list is used
    # at least once in our primary dataframe. returns the percentage of the master list that is indeed used in the primary dataframe
    #
    # params: column, master, master_column
    def score_every_master_value_used(self,object_name,p):
        df=self.get_dataframe(object_name)
        master = self.get_dataframe(p['master'])
        s=df[p['column']].to_list()

        m=set(master[p['master_column']].to_list())
        original_count=len(m)
        c=0
        for i in m:
            if i in s:
                m.remove(i)


        score = (original_count-len(m))/original_count

        return score

    # does a column contain only unique values
    #
    # param: column - the column to check
    def score_unique_column(self,object_name, p):
        df=self.get_dataframe(object_name)
        values = df[p['column']].to_list()
        values_set = set(values)
        if len(values) == len(values_set):
            return 1
        else:
            return 0

    # get the % of rows expected; if you get too many columns, it still returns a % showing your overage, upto a maximum
    # if you have double or more the number of rows desired, the returned score is 0
    #
    # param: expected_row_count - the number of expected rows
    def score_row_count(self,object_name,p):
        df=self.get_dataframe(object_name)
        row_count = len(df.index)
        score = row_count/p['expected_row_count']

        if score <= 1:
            return score
        if (score > 2):
            score = 0
        elif (score > 1):
            score = 2 - score

    # score a column for its match against a regular expression, returnint the % of rows that match the regex
    #
    # param: match - the regex to use, column - the column to match
    def score_match(self,object_name, p):
        df = self.get_dataframe(object_name)
        r = p['match']
        pattern = re.compile(r)
        score=0
        for i in df[p['column']]:
            if pattern.match(i):
                score+=1

        score = score/len(df[p['column']])
        return score

    def score_int(self,objectname, p):
        dt = self.get_dataframe(objectname)[p['column']].dtype
        if dt == np.dtype('int64'):
            return 1
        else:
            return 0

    def score_float(self,objectname, p):
        dt = self.get_dataframe(objectname)[p['column']].dtype
        if dt == np.dtype('float64'):
            return 1
        else:
            return 0

    def score_number(self,objectname, p):
        if self.score_float(objectname,p):
            return 1
        else:
            return self.score_int(objectname,p)

    file_system_provider = {
        'retrieve': retrieve_object_from_filesystem,
        'tag': lambda: None                             # filesystem provider doesn't currently support tagging
    }

    aws_provider = {
        'retrieve': retrieve_object_from_S3,
        'tag': update_object_tagging
    }

    # print('----------------')
    # x=[
    #     ['ExampleCSV.csv',score_column_count,{"expected_n":8},1,1],
    #     ['ExampleCSV.csv',score_row_count,{"expected_row_count":5},1,1],
    #     ['ExampleCSV.csv',score_column_names,{'columns':['Column 1', 'Second Column', 'Column 3', 'Optional Column 1',
    #                                             'Optional Column2', 'GroupByColumn','Integer Column', 'Float Column']},1,1],
    #     ['ExampleCSV.csv',score_no_blanks,{'column':'Second Column'},1,1],
    #     ['ExampleCSV.csv',score_float,{'column':'Float Column'},1,1],
    #     ['groups.csv',score_column_count,{"expected_n":1},1,1],
    #     ['ExampleCSV.csv',score_column_valid_values,{'column':'GroupByColumn', 'master':'groups.csv',
    #                                                     'master_column':'groups'},1,1],
    #     ['ExampleCSV.csv', score_unique_column, {'column': 'GroupByColumn'}, 1, 1],
    #     ['groups.csv',score_unique_column,{'column':'groups'},1,1],
    #     ['groups.csv',score_row_count,{'expected_row_count':3},1,1],
    #     ['groups.csv',score_match,{'column':'groups', 'match':'Group\d'},1,1],
    #
    # ]


    # q = validate_objects(x)
    # logger.info('Total average quality measure: {}'.format(q[0]))