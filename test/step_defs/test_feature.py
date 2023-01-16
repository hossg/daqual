import pytest
import daqual
import pandas as pd
from pytest_bdd import scenarios, given, when, then, parsers
import json
import os
import logging

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

scenarios('../features/single_file_quality.feature')

@given('the daqual framework', target_fixture='framework')
def framework():
    return {'dq': None}

# @pytest.fixture
@when(parsers.parse('a developer creates a daqual instance with a filename "{filename}"'))
def dq(filename,framework):
    d = daqual.daqual2(filename)
    framework['dq'] = d

@then(parsers.parse('the daqual instance will have a unique identifier "{id}" corresponding to the SHA1 hash of the contents of the file "{filename}"'))
def create_dq_object(filename,id, framework):
    dq = framework['dq']
    df = dq.df
    assert type(df) == pd.DataFrame
    assert dq.hash == id
    assert dq.filename == filename




# @given(parsers.parse('A daqual2 object for the file: "{filename}"'))
# @then(parsers.parse('return a quality score (1,2,3,4)'))
# @when(parsers.parse('a developer measures the quality with the configuration: "{param}"'))
# def score(param, dq):
#     return True

@given(parsers.parse('a daqual instance created from the file "{filename}"'), target_fixture="instance")
def instance(filename):
    d = daqual.daqual2(filename)
    return d

@pytest.fixture
@when(parsers.parse('a developer calls the score() function with the parameter {param}'))
def score(instance, param):
    param = json.loads(param)
    print(param)
    return instance.score(param)

@then("a tuple of 3 elements is returned")
def step_impl(score):
    assert type(score) == tuple
    assert len(score) == 3

ordinal = lambda n: int(n[:-2])  # convenience function to strip off the final two characters of '1st', '2nd', '3rd', etc
# TODO write a quick library function to do this for text versions as well, e.g. 'first', 'second', 'third',...

@then(parsers.parse('the "{nth}" element is "{m:f}"'))
def step_impl(score,nth,m):
    nth = ordinal(nth)
    assert score[nth-1]==m

