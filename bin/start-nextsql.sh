#!/bin/bash

# Source this file from the $NS_HOME directory to
# setup your environment. If $NS_HOME is undefined
# this script will set it to the current working directory.

export JAVA_HOME=${JAVA_HOME-/usr/local/lib/java7}
if [ ! -d $JAVA_HOME ] ; then
    echo "Error! JAVA_HOME must be set to the location of your JDK!"
    exit 1
fi

JAVA=${JAVA-${JAVA_HOME}/bin/java}

if [ -z $NS_HOME ]; then
    this=${0/-/} # login-shells often have leading '-' chars
    shell_exec=`basename $SHELL`
    if [ "$this" = "$shell_exec" ]; then
        # Assume we're already in NS_HOME
        interactive=1
        NS_HOME="$(pwd)/.."
    else
        interactive=0
        while [ -h "$this" ]; do
            ls=`ls -ld "$this"`
            link=`expr "$ls" : '.*-> \(.*\)$'`
            if expr "$link" : '.*/.*' > /dev/null; then
                this="$link"
            else
                this=`dirname "$this"`/"$link"
            fi
        done

        # convert relative path to absolute path
        bin=`dirname "$this"`
        script=`basename "$this"`
        bin=`cd "$bin"; pwd`
        this="$bin/$script"

        NS_HOME=`dirname "$bin"`
    fi
fi
export NS_HOME

# explicitly change working directory to $NS_HOME
cd $NS_HOME

GCLOGFILE=nextsql-gc-$(date +%Y%m%d-%H%M%S).log
JVMARGS=${JVMARGS-"-enableassertions -enablesystemassertions -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly -Xms1g -Xmx1g -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -verbose:gc -Xloggc:$NS_HOME/logs/${GCLOGFILE} -DNS_HOME=${NS_HOME}"}
export NS_CONF_DIR=$NS_HOME/conf
export NS_BUILD_DIR=$NS_HOME/src/build
export PATH=$NS_HOME/bin:$PATH
echo "$NS_BUILD_DIR"
CLASSPATH=$NS_CONF_DIR:$CLASSPATH
CLASSPATH=$NS_BUILD_DIR:$CLASSPATH
for jar in `ls ${NS_BUILD_DIR}/*.jar`; do
  CLASSPATH=${CLASSPATH}:$jar
done
for jar in `ls ${NS_BUILD_DIR}/dependency/*.jar`; do
  CLASSPATH=${CLASSPATH}:$jar
done
export CLASSPATH
export HOSTNAME=`hostname`

echo "NS_HOME                = $NS_HOME"
echo "JAVA_HOME              = $JAVA_HOME"
echo "CLASSPATH              = $CLASSPATH"
echo "JVMARGS                = $JVMARGS"
echo "HOSTNAME               = $HOSTNAME"

rm -f $NS_HOME/logs/nextsql-gc.log
ln -s ${GCLOGFILE} $NS_HOME/logs/nextsql-gc.log

echo "start Nextsql server..."
if [ x${NS_FOREGROUND} = x ]; then
exec nohup $JAVA $JVMARGS -DHOSTNAME="${HOSTNAME}" -classpath "$CLASSPATH" org.apache.nextsql.server.NextSqlServer "$@" > /dev/null 2>&1 &
else
$JAVA $JVMARGS -DHOSTNAME="${HOSTNAME}" -classpath "$CLASSPATH" org.apache.nextsql.server.NextSqlServer "$@"
fi
