#!/bin/bash
/usr/java/jdk1.8.0/bin/java -cp "target/*:target/dependency/*" -Djava.util.logging.config.file=src/main/resources/conf/logging.properties majordodo.broker.BrokerMain $@
