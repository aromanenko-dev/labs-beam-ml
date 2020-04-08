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

# Execute code

## Java only

    cd java

### Direct Runner

    mvn exec:java -Dexec.mainClass=com.talend.labs.beam.classification.ClassificationPipeline -Pdirect-runner -Dexec.args="--runner=DirectRunner --expansionServiceURL=localhost:8097 --useExternal=false"

### Flink Runner

    mvn exec:java -Dexec.mainClass=com.talend.labs.beam.classification.ClassificationPipeline -Pflink-runner -Dexec.args="--runner=FlinkRunner --expansionServiceURL=localhost:8097 --useExternal=false"

### Spark Runner

    mvn exec:java -Dexec.mainClass=com.talend.labs.beam.classification.ClassificationPipeline -Pspark-runner -Dexec.args="--runner=SparkRunner --expansionServiceURL=localhost:8097 --useExternal=false"

### Portable Spark Runner

Run the Portable Job Server from the main Beam git branch of the given version.

    ./gradlew :runners:spark:job-server:runShadow

Build the pipeline
    
    mvn clean package -Pportable-runner
    
Run the pipeline

    mvn exec:java -Dexec.mainClass=com.talend.labs.beam.classification.ClassificationPipeline -Pportable-runner -Dexec.args="--runner=PortableRunner --jobEndpoint=localhost:8099"


TODO

## Python only

### Direct Runner (Python)

    python classificationpipeline.py --runner DirectRunner

### Flink Runner

Run the Portable Job Server from the docker image:

    docker run --net=host apache/beam_flink1.9_job_server:2.19.0

Run the pipeline

    python classificationpipeline.py --runner PortableRunner --job_endpoint localhost:8099 --environment_type LOOPBACK

### Spark Runner

Run the Portable Job Server from the main Beam git branch of the given version.
Note: A docker image will be available soon.

    ./gradlew :runners:spark:job-server:runShadow

Run the pipeline

     python classificationpipeline.py --runner PortableRunner --job_endpoint localhost:8099 --environment_type LOOPBACK

## Cross-language Pipeline (Java calls python in the middle)

### Portable Spark Runner

Run the Portable Job Server from the main Beam git branch of the given version.

    ./gradlew :runners:spark:job-server:runShadow

Run the Expansion Service with user Python transform

     python talend/labs/beam/ml/expansion_service.py -p 9097
     
Run the pipeline
     
     mvn exec:java -Dexec.mainClass=com.talend.labs.beam.classification.ClassificationPipeline -Pportable-runner \
        -Dexec.args="--runner=PortableRunner --jobEndpoint=localhost:8099 --useExternal=true --expansionServiceURL=localhost:9097"
    
    
