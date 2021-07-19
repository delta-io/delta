## Delta Lake examples
In this folder there are examples taken from the delta.io quickstart guide and docs. They are available in both Scala and Python and can be run if the prerequisites are satisfied.

### Prerequisites
* For Python examples, PySpark 3.0.0 or above needs to be installed.
* For Scala examples, Spark does not need to be install because it depends on Spark maven artifacts.

### Instructions
* To run an example in Python run `spark-submit --packages io.delta:delta-core_2.12:0.7.0 PATH/TO/EXAMPLE`
* To run the Scala examples, `cd examples/scala` and run `./build/sbt "runMain example.{Example class name}"` e.g. `./build/sbt "runMain example.Quickstart"`
