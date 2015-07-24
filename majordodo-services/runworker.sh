#!/bin/bash
/usr/java/jdk1.8.0/bin/java -cp "target/*:target/dependency/*" majordodo.worker.WorkerMain $@
