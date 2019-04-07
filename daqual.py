import pandas as pd

# setup logging config
import logging
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',level=logging.INFO)
logger = logging.getLogger(__name__)

# setup AWS
import boto3
import botocore
BUCKET_NAME = 'daqual' # replace with your bucket name
s3 = boto3.resource('s3')

# retrieve objects from S3
def getObjectAsDF(objectkey):
    tempfilename='./temp/' + objectkey
    try:
        s3.Bucket(BUCKET_NAME).download_file(objectkey, tempfilename)
    except botocore.exceptions.ClientError as e:
        logger.error("Could not retrieve object {}".format(objectkey))
        return(0, None)
    df = pd.read_csv(tempfilename)
    # TODO - tidy up/remove tempfile
    logger.info("Retrieved and converted object {}".format(objectkey))
    return (1,df)

def update_object_tagging(objectkey, tag, value):
    tags = boto3.client('s3').get_object_tagging(Bucket=BUCKET_NAME,Key=objectkey)
    t = tags['TagSet']
    for i in t:
        if i['Key']==tag:
            i['Value']=str(value)
        else:
            t.append({'Key': tag, 'Value': str(value)})
    ts = {'TagSet':t}
    boto3.client('s3').put_object_tagging(Bucket=BUCKET_NAME, Key=objectkey, Tagging=ts)


def set_quality_score(objectkey, quality):
    logger.info("Setting quality_score on {} to {}".format(objectkey,quality))
    update_object_tagging(objectkey,'quality_score', quality)


# TODO - refactor to pass around object names as the key, rather than dataframes to allow alternate implementations not involving pandas
dataframes = {}
def get_dataframe(object_name):
    return dataframes[object_name]['dataframe']

# each item in the object list is thus:
# [object_name, check_function, function parameter, weight, threshold]
def check_objects(object_list):

    # first retrieve all required objects, create dataframes for them

    for item in object_list:
        if item[0] not in dataframes.keys():    # if we haven't already retrieved and processed this object
            score,df = getObjectAsDF(item[0])
            setattr(df,'objectname',item[0])
            if score == 1:
                dataframes[item[0]]={}
                dataframes[item[0]]['dataframe']=df
                dataframes[item[0]]['quality'] = 0
                dataframes[item[0]]['n_tests'] = 0
            if score == 0:
                return 0            # each array is defined such that ALL files must exist


    # if we have all required files, then for each entry in the list, we perform the requisite test
    # and for each individual file we keep track of the cumulative quality score for that file
    for item in x:
        dataframes[item[0]]['n_tests']+=1
        individual_test_score = item[1](dataframes[item[0]]['dataframe'], item[2])

        individual_threshold = item[4]
        individual_weight = item[3]
        if (individual_test_score < individual_threshold):
            logger.info("Threshold failure: {} {}({}) scored {}, expecting at least {}".format(
                item[0],item[1].__name__,item[2],individual_test_score,individual_threshold))
        dataframes[item[0]]['quality'] += (individual_weight * individual_test_score)



    average_quality = 0
    for i in dataframes.keys():
        dataframes[i]['quality'] /= dataframes[i]['n_tests']
        set_quality_score(i,dataframes[i]['quality'])
        average_quality += dataframes[i]['quality']
        logger.info("{}: quality: {}, n_tests: {}".format(i, dataframes[i]['quality'], dataframes[i]['n_tests']))
    average_quality /= len(dataframes)

    return average_quality, dataframes

# get the % of columns expected; if you get too many columns, it still returns a % showing your overage, upto a maximum
# if you have double or more the number of colums desired, the returned score is 0
#
# param: expected_n - the number of expected columns
def score_column_count(dataframe, p):
    score=len(dataframe.columns)/p['expected_n']
    if score <= 1:
        return score
    if (score > 2):
        score = 0
    elif (score > 1):
        score = 2 - score

    logger.warn("Object {} has {} columns, expecting only {}".format(dataframe.objectname,len(dataframe.columns),p['expected_n']))


# is a mandatory column complete?
#
# param: column name to check
def score_no_blanks(df, p):
    if p['column'] in df.columns[df.isna().any()].tolist():
        return 0
    else:
        return 1

# are the column names as expected?
#
# param: columns - a list of expected columns
def score_column_names(df, p):
    if (df.columns.to_list() == p['columns']):
        return 1
    else:
        return 0

# provide a master data frame, and column therein, and confirm the % of values from our regular dataframe, and column,
# that are present in that master set. useful check of referential integrity and consistency across files
#
# params: column, master, master_column
def score_column_valid_values(df,p):

    master = get_dataframe(p['master'])
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
def score_every_master_value_used(df,p):
    master = get_dataframe(p['master'])
    s=df[p['column']].to_list()

    m=set(master[p['master_column']].to_list())
    original_count=len(m)
    c=0
    for i in m:
        if i in s:
            m.remove(i)

    original
    score = (original_count-len(m))/original_count

    return score

# does a column contain only unique values
#
# param: column - the column to check
def score_unique_column(df, p):
    values = df[p['column']].to_list()
    values_set = set(values)
    if len(values) == len(values_set):
        return 1
    else:
        return 0



print('----------------')
x=[
    ['ExampleCSV.csv',score_column_count,{"expected_n":8},1,1],\
    ['ExampleCSV.csv',score_column_names,{'columns':['Column 1', 'Second Column', 'Column 3', 'Optional Column 1','Optional Column2', 'GroupByColumn',\
        'Integer Column', 'Float Column']},1,1],\
    ['ExampleCSV.csv',score_no_blanks,{'column':'Second Column'},1,1],\
    ['groups.csv',score_column_count,{"expected_n":1},1,1],\
    ['ExampleCSV.csv',score_column_valid_values,{'column':'GroupByColumn', 'master':'groups.csv', 'master_column':'groups'},1,1],
    ['ExampleCSV.csv', score_unique_column, {'column': 'GroupByColumn'}, 1, 1],
    ['groups.csv',score_unique_column,{'column':'groups'},1,1]

]


q = check_objects(x)
logger.info('Total average quality measure: {}'.format(q[0]))