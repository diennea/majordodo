#!/bin/bash
/usr/java/jdk1.8.0/bin/java -cp "target/*:target/dependency/*" dodo.broker.BrokerMain conf/broker.properties
