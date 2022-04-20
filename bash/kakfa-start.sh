~/kafka_2.13-3.1.0/bin/zookeeper-server-start.sh ~/kafka_2.13-3.1.0/config/zookeeper.properties &
JMX_PORT=8004 ~/kafka_2.13-3.1.0/bin/kafka-server-start.sh ~/kafka_2.13-3.1.0/config/server.properties &
~/CMAK/target/universal/cmak-3.0.0.6/bin/cmak -Dconfig.file=~/CMAK/target/universal/cmak-3.0.0.6/conf/application.conf -Dhttp.port=8081 &