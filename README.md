README
======

Simple example of how to integrate a Python based PTransform in an Apache Beam Java pipeline. This uses the ExpansionService to enable cross-language transforms. Interesting use cases for this is the ability to wrap ready to use components that integrate transforms for the rich ecosystem of Machine Learning (ML) libraries in Beam pipelines.

For this example we will integrate a simple scikit based top k-means algorithm as proof of concept but this can be applied to other libraries.

# Developer info

You should have a working Python and Java development environment.

## Java

    cd java
    mvn clean install

## Python setup

This code has only been tested with Python 3.7.x

    cd python
    pip install -r requirements.txt

You can auto-format the code by running:
    
    black .

# Execution

