# daqual
A data quality framework for python

## Antiscope

Daqual is a framework only for measuring and assessing data quality in a data-handling system. It does not involve itself in the complexities of what to do with data files or objects once a quality measure has been determined (e.g. alert, re-run, fix-data, etc).

## High level model and key concepts

Daqual is a framework that encourages reuse and composability, made up of functions that each perform a trivial quality measure/assessment.  The power of the framework emerges from the composability and reuse of these simple functions.

1.	Each quality check/function returns a score (of between 0 and 1) for quality; 1 being perfect, 0 being failure. The semantic intent of the quality score may vary between functions, in some cases representing a "percentage", say, of an expected value, or with other predetermined values in the interval (0,1) representing particular levels or measures of quality.
2.	Although a number of quality scoring functions are provided, it is intended that custom functions can be easily written and used in the same framework.
3.  Quality scoring functions can call other functions, aggregating or combining the scores derived from called subordinate functions.
4.  Quality functions operate upon objects (typically file objects or objects retrieved from an object store, e.g. AWS S3).
5.	Multiple objects and quality functions can be assessed in aggregate and as an atomic unit, returning both an overall quality score, individual scores for particular tests, as well as individual or aggregate "pass or fail" measures, defined by a threshold for each test or for the entire set.
6.  Aggregate scoring can also take advantage of "weighting" of individual tests, to ensure that some tests are treated as more or less significant than others.
7.  Once an object has been assessed, it can be tagged with a record of it's quality score.  
8.  Scoring functions can take other objects as parameters to allow quality measures to be determined with reference to other data sources, e.g. to ensure mutual consistency between objects.  
9.  Different object-stores are implemented by use of the concept of *provider*.  Providers for AWS S3 and local filesystem are implemented by default, but other providers can be trivially implemented and incorporated.

## Table of Implemented/Proposed Quality Checks

Here are a list of data quality functions implemented in Daqual core, that are needed to implement a basic framework which can then be combined to provide us with the overall functionality we desire.  

Description                                         | Function Name                 | Parameters                    | Notes             
-----------                                         | -------------                 | ----------                    | -----             
Expected number of columns                          | score_column_count            | column, expected_n            |
Expected column names                               | score_column_names            | columns                       | returns either 0 or 1
Are blanks allowed in a column?                     | score_no_blanks               | column                        | returns either 0 or 1
Is a column constrained to a fixed set of values?   | score_column_valid_values     | column, master, master_column | returns % of values that are present in master list
Is every single value from a master table used?     | score_every_master_value_used | column, master, master_column | returns % of values that are used from a master list
Are all the values in a column unique?              | score_unique_column           | column                        | returns either 0 or 1
Column matches a regex                              | score_match                   | match, column                 |
Column is an integer                                | score_int                     | column                        | returns either 0 or 1; ignores NaN
Column is a date                                    |                               |
Column is a float                                   | score_float                   | column                        | returns either 0 or 1; ignores NaN
Column is a number                                  | score_number                  | column                        | returns either 0 or 1; ignores NaN
Expected sum of a column (including group-by)       |                               |
Expected mean of a column (including group-by)      |                               |
Row count                                           | expected_row_count            | expected_row_count or comparison, expected_delta
Max of a column (including group-by)                |
Min of a column (including group-by)                |
Compare to another version of a file                |
Are all values greater than?                        |
Are all values less than?                           |
Number of different values in a column              |

*Note: parameters are supplied in a *dict**

See source code for detailed explanation of how to use each scoring function

Definitions/explanations of common parameters:
* column
* master
* master_column
* comparison 
* expected
* expected_row_count
* expected_n
* match

## Example Testset

A test set is defined as an array, with each element being structured as follows:
```[object_name, scoring_function, function parameter object/dict, weight, threshold]```

### Example:

```
test_set=[
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
    ['daqual/ExampleCSV-2.csv',daqual.score_column_count,{"expected_n":8},1,1],
    ['daqual/ExampleCSV.csv',daqual.score_row_count,{"comparison":'daqual/ExampleCSV-2.csv', 'expected_delta':1},1,1],
]
```

