#!/bin/bash 

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
sync_conf=$base/conf/canal-es.properties
loader_path=$base/conf
logback_configurationFile=$base/conf/logback.xml
export LANG=en_US.UTF-8
export BASE=$base

if [ -f $base/bin/totoro.pid ] ; then
	echo "found totoro.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

if [ ! -d $base/logs/totoro ] ; then
	mkdir -p $base/logs/totoro
fi

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

if [ -z "$JAVA" ]; then
  echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.8) in your PATH." 2>&2
  exit 1
fi

case "$#" 
in
0 ) 
	;;
1 )	
	var=$*
	if [ -f $var ] ; then 
		sync_conf=$var
	else
		echo "THE PARAMETER IS NOT CORRECT.PLEASE CHECK AGAIN."
        exit
	fi;;
2 )	
	var=$1
	if [ -f $var ] ; then
		sync_conf=$var
	else 
		if [ "$1" = "debug" ]; then
			DEBUG_PORT=$2
			DEBUG_SUSPEND="n"
			JAVA_DEBUG_OPT="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=$DEBUG_PORT,server=y,suspend=$DEBUG_SUSPEND"
		fi
     fi;;
* )
	echo "THE PARAMETERS MUST BE TWO OR LESS.PLEASE CHECK AGAIN."
	exit;;
esac

str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
	JAVA_OPTS="-server -Xms2048m -Xmx3072m -Xmn1024m -XX:SurvivorRatio=2 -XX:PermSize=96m -XX:MaxPermSize=256m -Xss256k -XX:-UseAdaptiveSizePolicy -XX:MaxTenuringThreshold=15 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:+HeapDumpOnOutOfMemoryError"
else
	JAVA_OPTS="-server -Xms1024m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m "
fi

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
CANAL_OPTS="-Dloader.path=$loader_path -DappName=totoro-canal-elasticsearch -Dlogback.configurationFile=$logback_configurationFile"
MAIN_JAR=$base/lib/canal-elasticsearch-*.jar

if [ -e $sync_conf -a -e $logback_configurationFile ]
then 
	
	for i in $base/lib/*;
		do CLASSPATH=$i:"$CLASSPATH";
	done
 	CLASSPATH="$base/conf:$CLASSPATH";
 	
 	echo "cd to $bin_abs_path for workaround relative path"
  	cd $bin_abs_path
 	
	echo LOG CONFIGURATION : $logback_configurationFile
	echo sync conf : $sync_conf
	echo CLASSPATH :$CLASSPATH
	$JAVA $JAVA_OPTS $JAVA_DEBUG_OPT $CANAL_OPTS -classpath .:$CLASSPATH com.totoro.canal.es.TotoroLauncher 1>>$base/logs/totoro.log 2>&1 &

	echo $! > $base/bin/totoro.pid
	
	echo "cd to $current_path for continue"
  	cd $current_path
else 
	echo "sync conf("$sync_conf") OR log configration file($logback_configurationFile) is not exist,please create then first!"
fi
