# Created by hossein at 07/01/2021
Feature: Quality scoring of a single, isolated File
  As an ETL developer
  I want to call the score function to obtain a quality score
  So that I can measure the data quality of a file

  Scenario: Load a file into the daqual framework
    Given the daqual framework
    When a developer creates a daqual instance with a filename "examples/daqual/iso-currencies.csv"
    Then the daqual instance will have a unique identifier "a46133e90d677e82e1e6da3854d97d140231148d" corresponding to the SHA1 hash of the contents of the file "examples/daqual/iso-currencies.csv"

#  Scenario: A daqual2 object has the correct fields
#    Given A daqual2 object for the file: "examples/daqual/iso-currencies.csv"
#    When a developer measures the quality with the configuration: {'fields': {'ENTITY','Currency','Alphabetic Code','Numeric Code','Minor unit'}}
#    When non-existing clause
#    Then return a quality score (1,2,3,4)


  Scenario: The score function returns a tuple of quality metrics
    Given a daqual instance created from the file "examples/daqual/iso-currencies.csv"
    When a developer calls the score() function with the parameter {"fields": {"ENTITY":1,"Currency":1,"Alphabetic Code":1,"Numeric Code":1,"Minor unit":1}, "expected_rows":279}
    Then a tuple of 3 elements is returned
    And the "1st" element is "1.0"
    And the "2nd" element is "1.0"
    And the "3rd" element is "3.0"

  Scenario: The score function can be a maximum of number of rows divided by expected number of rows
    Given a daqual instance created from the file "test/features/iso-currencies-101.csv"
    When a developer calls the score() function with the parameter {"fields": {"ENTITY":1,"Currency":1,"Alphabetic Code":1,"Numeric Code":1,"Minor unit":1}, "expected_rows":100}
    Then a tuple of 3 elements is returned
    And the "1st" element is "0.99"

  Scenario: The score function counts an excess of rows with a corresponding reduction in %
    Given a daqual instance created from the file "test/features/iso-currencies-99.csv"
    When a developer calls the score() function with the parameter {"fields": {"ENTITY":1,"Currency":1,"Alphabetic Code":1,"Numeric Code":1,"Minor unit":1}, "expected_rows":100}
    Then a tuple of 3 elements is returned
    And the "1st" element is "0.99"