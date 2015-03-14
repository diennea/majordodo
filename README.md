# majordodo
Distributed Operation and Data Organizer

Majordodo is a Distributed Resource Manager, essentially made of a groups of brokers which coordinate a pool of worker machines.

There is no single point of failure, brokers replicate state using Apahce Bookkeeper. Workers are handled in a very elastic way, that is, you can add and remove workers at runtime, workers can crash anytime and the system will continue to be available.

In its first release tasks are executed by Java-based workers, that is, the implementation of the actual work is to be coded using Java or any language which can be run on a JVM, such as Scala, Groovy, JRuby and son on...

