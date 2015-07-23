# [Majordodo](http://majordodo.org/)

Majordodo is a Distributed Resource Manager, essentially made of a groups of brokers which coordinate a pool of worker machines.

To get started, check out http://majordodo.org/

There is no single point of failure, brokers replicate state using Apache Bookkeeper. Workers are handled in a very elastic way, that is, you can add and remove workers at runtime, workers can crash anytime and the system will continue to be available.

In its first release tasks are executed by Java-based workers, that is, the implementation of the actual work is to be coded using Java or any language which can be run on a JVM, such as Scala, Groovy, JRuby and son on...

The basic concept of MD is Task: A Task is an operation which is to be executed by the system. Once a client submits a a tse execution request to MD the broker will gather the requested resources in the cluster and schedules the effective  execution to a worker jvm. the stalus of te task is tracked by the broker and if the worder crashes it's execution is routed to another worker transparently.


## License

Majordodo is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).