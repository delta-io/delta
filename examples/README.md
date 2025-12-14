## Delta Lake examples
In this folder there are examples taken from the delta.io quickstart guide and docs. They are available in both Scala and Python and can be run if the prerequisites are satisfied.

### Prerequisites
* See [Set up Apache Spark with Delta Lake](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake).

### Instructions
* To run an example in Python run `spark-submit --packages io.delta:delta-spark_2.12:{Delta Lake version} PATH/TO/EXAMPLE`
* To run the Scala examples, `cd examples/scala` and run `./build/sbt "runMain example.{Example class name}"` e.g. `./build/sbt "runMain example.Quickstart"`

### Docker Instructions

#### Building the image

* To build the image run: `docker build -t delta-lake-playground:latest .`

#### Running the image

* After the image is built run jupyterlab using: `docker run -it -p 8888:8888 delta-lake-playground`
* Open Jupyter in your browser `http://localhost:8888/lab?token=docker`

#### Sample Notebooks
* There are sample python notebooks which are ported in the notebooks folder.

#### Running Shells
* In JupyterLab click file and start a terminal
* In the terminal you can run `spark-shell`, `sparkR` or `pyspark` shell.
* Delta configurations are already set as a spark default conf

