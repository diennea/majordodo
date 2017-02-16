<p class="intro">
Majordodo is a replicated service to build complex multi-tenant computing grids. 
You can view it as a distributed threadpool where tasks are submitted by users and a network of brokers schedules execution on a large set of machines, caring about failures, recovery and priority-based resources assignment.<br></br>Tasks are implemented in Java and can be submitted and monitored using a simple HTTP REST JSON API. 
</p>

<h2>Multi-tenancy</h2>
Majordodo manages resources for each tenant and guarantees resource access isolation and fair scheduling. Users' execution priorities are dynamic and can be modified at runtime for each and every task, even ones that have already been submitted to the scheduler.

<h2>Resiliency</h2>
Majordodo leverages <a href="http://zookeeper.apache.org/" >Apache Zookeeper </a> and <a href="http://bookkeeper.apache.org/" >Apache Bookkeeper </a>to build a fully replicated, shared-nothing, broker architecture without any single point of failure. Workers can be run on commodity-hardware, virtual machines or dynamically provisioned cloud resources which may fail at any time without service interruption.

<h2>Scalability</h2>
Majordodo is designed to manage tasks for thousands of hosts. It is simple to add and remove hosts and resources with dynamic reconfiguration and adaptive tasks scheduling.
 