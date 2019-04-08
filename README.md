# daqual
A data quality framework for python

## Antiscope

As we discussed the question of what to do when a quality measure fails a threshold is interesting and raises many operational questions, e.g. re-running, making changes, etc, and this in turn poses interesting questions re: master-data (and how that may change over time as well).  I touch on that later on, but initially I want to focus only on the framework for assessing quality irrespective of any processes for dealing with the results of that quality assessment.

## High level model

Daqual is a framework that encourages reuse and composability, made up of functions that each perform a trivial quality measure/assessment.  The power of the framework emerges from the composability and reuse of these simple functions.

1.	Each quality check/function returns a score (of between 0 and 1) for quality; 1 being perfect, 0 being failure.
2.	Each subordinate/child/called function does the same
3.	The calling function decides how to aggregate/weight or combine the scores from subordinate functions
4.	Examples of a weighting function might be a piecewise function, eg  delta(W,1) (or equivalently AND(W,1)) requiring W to be 1 to yield a 1 of its own
5.	The advantage of choosing 1 for success and 0 for failure is that it maps conveniently into python Boolean values for easy combination of binary quality scores (1’s and 0’s), which is likely to be a common use-case.

## Approach for composability

Although the quality functions can be called explicitly by a script, it is expected that instead arrays of funcitons and their parameters are constructed, and these are iterated over by a "controller" function. This could be refined further by saying you could pass both an assessment function, and a weighting function (for that assessment), in a map-reduce-type approach, for example:

QUALITY_CHECK= [[file_exists,1],[columns_exist_and_constant,1],[value_within_percentage(10),0.5]]

The framework should be flexible - allowing multiple ways of aggregating functions and combining scores.

## Complication

In addition to each function returning “just” a score, we also need to think how to return other content or data aggregation from the file (i.e. data or data summary) which we might want to use to help validate or score other files or to ensure consistency across files. A function might trivially achieve this by returning a tuple of a score and an object. The latter could be ignored by calling functions that don’t know the semantics of the object being returned.  

As an example consider two files supplied by a system, one containing Asset records and one containing Position records.  Perhaps we want to assess or assert the Position file contains positions only for assets in the Asset file.  In which case the data quality score of the Asset file might return (1,[“asset-id-1”,”asset-id-2”,”asset-id-3”]).

## Quality Checks

Here are a list of data quality functions that are needed to implement a basic framework which can then be combined to provide us with the functionality we desire.  

1.	File existence/absence. 
a.	Multiple file atomic existence (all or none)
b.	Is the file what was expected... Value Date, contents?

2.	What columns to expect: number and name, does every row have that number of columns?

3.	Validity of values – what constraints can cells have? Blanks allowed in any places? Regex matches? (string, etc)? Constrained to a master list? Dependent on another value(s) from another file? (consider multi-file consistency in a generic way?)
4.	Number of rows to have. Fixed? Within absolute or relative to yesterday or to another file?

5.	Number of different values in a column

6.	Max or min value in a column

## Table of Implemented/Proposed Quality Checks

Description                                         | Function Name                 | Parameters
-----------                                         | -------------                 | ----------
Expected number of columns                          | score_column_count            |
Expected column names                               | score_column_names            |
Are blanks allowed in a column?                     | score_no_blanks               |
Is a column constrained to a fixed set of values?   | score_column_valid_values     |
Is every single value from a master table used?     | score_every_master_value_used |
Are all the values in a column unique?              | score_unique_column           |
Column meets a regex                                | score_match                   |
Column is an integer                                | score_int                     |
Column is a date                                    |                               |
Column is a float                                   | score_float                   |
Column is a number                                  | score_number                  |
Expected sum of a column (including group-by)       |                               |
Expected mean of a column (including group-by)      |                               |
Row count                                           | expected_row_count            |
Max of a column (including group-by)                |
Min of a column (including group-by)                |
Compare to another version of a file                |
Are all values greater than?                        |
Are all values less than?                           |




## Actions and processes

We will also need to come to the question of what to do when a DQ check fails (i.e. has a DQ score of <1 and/or <threshold-defined-for-that-file). Again, I think we should have some standard templates for how to handle this:
•	Abort
•	Proceed regardless, but simply record the DQ score on the metadata of the file 
•	Accumulate failure score points over time to measure quality of the process?  
•	Warn (e.g. via email, other)

We also need to consider more general file handling – what if a file is duplicated or repeated unexpectedly (e.g. same business date, but with new – or repeated - data).  Need to consider a general template to handle common options, e.g. ignore the file, warn, replace downstream data, etc.

## Re-running checks and processes 

We also need to consider the situation when we want to re-run a process it may be that the original “failure” was due to an error in the data file, or an error in a dependent master data file used for lookups/validation.  Given in principle, either could be the case, we want to allow for the possibility of re-running where one or the other, or both, has changed. This does not mean though we should never denormalise files as part of processing and always retain lookup-ids-only.  Instead we should be able to regenerate and replace files (denormalised or not) if we find ourselves unhappy with the quality of a file, or any of its antecedents.
