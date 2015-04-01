#!/bin/bash
/usr/java/current/bin/java -cp "target/dependency/*" dodo.worker.WorkerMain conf/worker.properties
