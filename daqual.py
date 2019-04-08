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

# retrieve objects from S3 and return a pandas DataFrame
def retrieveObjectFromProvider(objectkey):
    tempfilename='./temp/' + objectkey
    try:
        s3.Bucket(BUCKET_NAME).download_file(objectkey, tempfilename)
    except botocore.exceptions.ClientError as e:
        logger.error("Could not retrieve object {}".format(objectkey))
        return(0, None)
    df = pd.read_csv(tempfilename)
    # noTODO - tidy up/remove tempfile; actually no - because we may want to use the actual file objects directly
    # instead of only accessing via dataframes
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
object_list = {}
def get_dataframe(object_name):
    return object_list[object_name]['dataframe']

# each item in the object list is thus:
# [object_name, check_function, function parameter, weight, threshold]
def validate_objects(validation_list):

    # first retrieve all required objects, create dataframes for them

    for item in validation_list:
        if item[0] not in object_list.keys():    # if we haven't already retrieved and processed this object
            score,df = retrieveObjectFromProvider(item[0])
            setattr(df,'objectname',item[0])
            if score == 1:
                object_list[item[0]]={} # create the key and the dict
                object_list[item[0]]['dataframe']=df
                object_list[item[0]]['quality'] = 0
                object_list[item[0]]['n_tests'] = 0
            if score == 0:
                return 0            # each array is defined such that ALL files must exist


    # if we have all required files, then for each entry in the list, we perform the requisite test
    # and for each individual file we keep track of the cumulative quality score for that file
    for item in x:
        object_list[item[0]]['n_tests']+=1
        individual_test_score = item[1](item[0], item[2])

        individual_threshold = item[4]
        individual_weight = item[3]
        if (individual_test_score < individual_threshold):
            logger.info("Threshold failure: {} {}({}) scored {}, expecting at least {}".format(
                item[0],item[1].__name__,item[2],individual_test_score,individual_threshold))
        object_list[item[0]]['quality'] += (individual_weight * individual_test_score)

    average_quality = 0
    for i in object_list.keys():
        object_list[i]['quality'] /= object_list[i]['n_tests']
        set_quality_score(i, object_list[i]['quality'])
        average_quality += object_list[i]['quality']
        logger.info("{}: quality: {}, n_tests: {}".format(i, object_list[i]['quality'], object_list[i]['n_tests']))
    average_quality /= len(object_list)

    return average_quality, object_list

# get the % of columns expected; if you get too many columns, it still returns a % showing your overage, upto a maximum
# if you have double or more the number of colums desired, the returned score is 0
#
# param: expected_n - the number of expected columns
def score_column_count(object_name, p):
    dataframe = get_dataframe(object_name)
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
def score_no_blanks(object_name, p):
    df=get_dataframe(object_name)
    if p['column'] in df.columns[df.isna().any()].tolist():
        return 0
    else:
        return 1

# are the column names as expected?
#
# param: columns - a list of expected columns
def score_column_names(object_name, p):
    df=get_dataframe(object_name)
    if (df.columns.to_list() == p['columns']):
        return 1
    else:
        return 0

# provide a master data frame, and column therein, and confirm the % of values from our regular dataframe, and column,
# that are present in that master set. useful check of referential integrity and consistency across files
#
# params: column, master, master_column
def score_column_valid_values(object_name,p):
    df=get_dataframe(object_name)

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
def score_every_master_value_used(object_name,p):
    df=get_dataframe(object_name)
    master = get_dataframe(p['master'])
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
def score_unique_column(object_name, p):
    df=get_dataframe(object_name)
    values = df[p['column']].to_list()
    values_set = set(values)
    if len(values) == len(values_set):
        return 1
    else:
        return 0



print('----------------')
x=[
    ['ExampleCSV.csv',score_column_count,{"expected_n":8},1,1],
    ['ExampleCSV.csv',score_column_names,{'columns':['Column 1', 'Second Column', 'Column 3', 'Optional Column 1',
                                            'Optional Column2', 'GroupByColumn','Integer Column', 'Float Column']},1,1],
    ['ExampleCSV.csv',score_no_blanks,{'column':'Second Column'},1,1],
    ['groups.csv',score_column_count,{"expected_n":1},1,1],
    ['ExampleCSV.csv',score_column_valid_values,{'column':'GroupByColumn', 'master':'groups.csv',
                                                    'master_column':'groups'},1,1],
    ['ExampleCSV.csv', score_unique_column, {'column': 'GroupByColumn'}, 1, 1],
    ['groups.csv',score_unique_column,{'column':'groups'},1,1]

]


q = validate_objects(x)
logger.info('Total average quality measure: {}'.format(q[0]))