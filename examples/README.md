## Delta Lake examples
In this folder there are examples taken from the delta.io quickstart guide and docs. They are available in both Scala and Python and can be run if the prerequisites are satisfied.

### Prerequisites
* Apache Spark version 2.4.2 or above
* PySpark is required for running python examples

### Instructions
* To run an example in Python run `spark-submit --packages io.delta:delta-core_2.11:0.4.0 PATH/TO/EXAMPLE`
* To run the Scala examples, `cd examples/scala` and run `./build/sbt "runMain example.{Example class name}"` e.g. `./build/sbt "runMain example.Quickstart"`