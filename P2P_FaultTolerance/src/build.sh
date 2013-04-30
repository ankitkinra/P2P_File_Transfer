if [ $JAVA_HOME ]
then
echo "Starting build...."
javac -classpath ../lib/log4j-1.2.17.jar:../lib/xmlpull-1.1.3.1.jar:../lib/xpp3_min-1.1.4c.jar:../lib/xstream-1.4.4.jar org/umn/distributed/p2p/common/*.java org/umn/distributed/p2p/node/*.java org/umn/distributed/p2p/server/*.java org/umn/distributed/p2p/testfrmwrk/*.java
else
echo "JAVA_HOME not defined"
fi

