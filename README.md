README
======

Simple example of how to integrate a Python based PTransform in an Apache Beam Java pipeline. This uses the ExpansionService to enable cross-language transforms. Interesting use cases for this is the ability to wrap ready to use components that integrate transforms for the rich ecosystem of Machine Learning (ML) libraries in Beam pipelines.

For this example we will integrate a simple scikit based top k-means algorithm as proof of concept but this can be applied to other libraries.

# Execution

TODO

# Developer info

You should have a working environment with:

- Java 8
- Python 3.7.x

## Java

    mvn clean install

Please auto format your code by using the Google Java Style plugin or spotless.

## Python

This code has only been tested with Python 3.7.x

### Prepare a virtualenv for the project

    python3 -m venv ~/.virtualenvs/python3/labs-beam-ml
    source ~/.virtualenvs/python3/labs-beam-ml/bin/activate

### Install the project dependencies

    cd python
    pip install -r requirements.txt

You can auto-format the code by running:
    
    black .

