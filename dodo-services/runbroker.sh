#!/bin/bash
/usr/java/current/bin/java -cp "target/*:target/dependency/*" dodo.broker.BrokerMain conf/broker.properties
