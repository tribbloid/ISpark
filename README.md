ISpark
======

**ISpark** is an [Apache Spark-shell](http://spark.apache.org/) backend for [IPython](http://ipython.org).

**ISpark** is ported from [IScala](https://github.com/mattpap/IScala), all credit goes to [Mateusz Paprocki](https://github.com/mattpap)

## Requirements

* [IPython](http://ipython.org/ipython-doc/stable/install/install.html) 2.0+
* [Java](http://wwww.java.com) JRE 1.7+

## How it works

ISpark is a standard Spark Application that when submitted, its driver will maintain a three-way connection
between IPython UI server and Spark cluster.

## Demo

[Click me](http://ec2-54-165-231-62.compute-1.amazonaws.com:8888/notebooks/all_inclusive_do_not_create_new_notebook.ipynb) for a quick impression.

This environment is deployed on a Spark cluster with 4+ cores. It comes with no uptime guarantee and may not be accessible during maintenance.

## Usage

ISpark only supports [Native (Spark-shell) environment](http://spark.apache.org/docs/latest/quick-start.html), support for Mahout DRM
will be added upon request.

ISpark needs to be compiled and packaged into an uber jar by [Maven](http://maven.apache.org/) before being submitted and deployed:

```bash
./mvn-install.sh
...
Building jar: ${PROJECT_DIR}/core/target/ispark-core-${PROJECT_VERSION}.jar
...
```

after which you can define a `Spark` profile for IPython by running:
```bash
$ ipython profile create spark
```
Then adding the following line into `~/.ipython/profile_spark/ipython_config.py`:

```python
import os
c = get_config()

SPARK_HOME = os.environ['SPARK_HOME']
# the above line can be replaced with: SPARK_HOME = '${INSERT_INSTALLATION_DIR_OF_SPARK}'
MASTER = '${INSERT_YOUR_SPARK_MASTER_URL}'

c.KernelManager.kernel_cmd = [SPARK_HOME+"/bin/spark-submit",
 "--master", MASTER,
 "--class", "org.tribbloid.ispark.Main", 
 "--executor-memory", "2G", #ISpark driver takes more memory than most other Spark drivers
 "--jars", "${INSERT_FULL_PATH_OF_ISPARK_UBER_JAR}",
 "${INSERT_FULL_PATH_OF_OTHER_JARS}",
 "--profile", "{connection_file}",
 "--interp", "Spark",
 "--parent"]

c.NotebookApp.ip = '*' # only add this line if you want IPython-notebook being open to the public
c.NotebookApp.open_browser = False # only add this line if you want to suppress opening a browser after IPython-notebook initialization
c.NotebookApp.port = 8888
```

Congratulation! Now you can initialize ISpark CLI or ISpark-notebook by running:

`ipython console --profile spark` OR `ipython notebook --profile spark`

(Support for the data collection/enrichment engine SpookyStuff has been moved to an independent project: https://github.com/tribbloid/ISpooky.git)

## Example

```scala
In [1]: sc
Out[1]: org.apache.spark.SparkContext@2cd972df

In [2]: sc.parallelize(1 to 10).map(v => v*v).collect.foreach(println(_))
Out[2]:
1
4
9
16
25
36
49
64
81
100
```

## Magics

ISpark supports magic commands similarly to IPython, but the set of magics is
different to match the specifics of Scala and JVM. Magic commands consist of
percent sign `%` followed by an identifier and optional input to a magic. Magic
command's syntax may resemble valid Scala, but every magic implements its own
domain specific parser.

### Type information

To infer the type of an expression use `%type expr`. This doesn't require
evaluation of `expr`, only compilation up to _typer_ phase. You can also
get compiler's internal type trees with `%type -v` or `%type --verbose`.

```scala
In [1]: %type 1
Int

In [2]: %type -v 1
TypeRef(TypeSymbol(final abstract class Int extends AnyVal))

In [3]: val x = "" + 1
Out[3]: 1

In [4]: %type x
String

In [5]: %type List(1, 2, 3)
List[Int]

In [6]: %type List("x" -> 1, "y" -> 2, "z" -> 3)
List[(String, Int)]

In [7]: %type List("x" -> 1, "y" -> 2, "z" -> 3.0)
List[(String, AnyVal)]

In [8]: %type sc
SparkContext
```
### Warning

Support for sbt-based library/dependency management has been removed due to its incompatibility with spark deployment requirement.
if sbt is allowed to download new dependencies, using them in any distributed closure may compile
but will throw ClassDefNotFoundErrors in runtime because they won't be submitted to Spark master.
Users are encouraged to attach their jars using the "--jars" parameter of spark-submit.

## License

Copyright &copy; 2014 by Mateusz Paprocki, Peng Cheng and contributors.

Published under ASF License, see LICENSE.
