bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
sh bin/kafka-server-start.sh config/server.properties 1>/dev/null  2>&1  &
port:2190
netstat -lnp|grep 9092
bin/kafka-topics.sh --create --zookeeper localhost:2190 --replication-factor 1 --partitions 1 --topic calllog
bin/kafka-topics.sh --list --zookeeper localhost:2190
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
bin/kafka-console-consumer.sh --zookeeper localhost:2190 --topic calllog --from-beginning
/usr/local/flume/bin/flume-ng agent -n a1 -c /usr/local/flume/conf/ -f /root/calllog/flume-kafka.conf
如果hbase因为协处理器挂了需要改hbase-site.xml
hbase.coprocessor.enabled false（hbase-default.xml）
把注册了错误协处理器表删除
在ture