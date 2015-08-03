# [Majordodo](http://majordodo.org/)

MMajordodo is a Distributed Resource Manager, essentially consisting of brokers which coordinate a pool of workers.

There is no single point of failure, brokers replicate state using Apache BookKeeper. Workers are handled in a very elastic way: you can add and remove workers at runtime, they can crash at any time and the system will continue to be available. You can also add workers to distinct groups to handle different type of works or priorities.

The basic concept of Majordodo is the Task. A Task is an operation which is to be executed by the system. Once a client submits an execution request to Majordodo, a broker will gather the requested resources in the cluster and schedule the effective execution to a worker. The task status is tracked by the broker and if the worker crashes the execution is transparently routed to another worker. The priorities of the single tasks can be modified at runtime.

You can also assign a task to a slot. A Slot is like a task with a shared lock: when a client submit a task and request it to be assigned to a defined slot then the system will accept the request only if there isn't another tasks in waiting or running status for that slot. This can be usefull to implement simple distributed locks without the need of expensive distributed lock algorithms.

In the first release tasks are executed by Java-based workers: the implementation of the actual work is to be coded using Java or any other language which can be run on a JVM (such as Scala, Groovy, JRuby, ...).


## License

Majordodo is under [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).
