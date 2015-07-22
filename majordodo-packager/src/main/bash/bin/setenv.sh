# Basic Environment variables

#JAVA_HOME=
JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8223 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=true -Dcom.sun.management.jmxremote.password.file=jmxremote.password -Dcom.sun.management.jmxremote.access.file=jmxremote.access -Dcom.sun.management.jmxremote.ssl=false"
JAVA_OPTS="-Xmx1g -Xms1g -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=1g $JMX_OPTS"


if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=`which java 2>/dev/null`
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_BIN=`dirname $JAVA_PATH 2>/dev/null`
    JAVA_HOME=`dirname $JAVA_BIN 2>/dev/null`
  fi
  if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME environment variable is not defined and is needed to run this program"
    exit 1
  fi
fi
