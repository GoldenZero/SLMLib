# SLMLib
Spark Language Modelling Library

Spark Language Modelling Library (SLMLib) is a Scala library for the Spark framework. 
The key design objective was to provide robust n-gram processing functionality on very large corpora (100 billion words) for tasks like n-gram extraction, n-gram statistics with smoothing, KWIC concordance, and further functionalities
derived from the core tasks. The library is self-contained, in the sense that it supports the tokenization step, n-gram statistics generation, and an interactive analysis of result sets.


To build the SLMLib library, Java, Scala and sbt should be pre-installed.
The build steps:
0) tar xvf slmlib.tgz
1) cd SLMLib
2) sbt package

The build tool will output the full path to the slmlib_2.10-1.0.3.jar library.
The library can be either used from a standalone Scala program running on Spark cluster, or from the interactive Spark scala-shell.
