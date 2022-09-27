# TopicTransformer
Single Message Transforms (SMT) implementation for routing messages

### Build

     mvn clean install
     
### Usage

Compile this JAR and put it into a path specified in _plugin.path_ on each node running Kafka Connect, then restart the workers.

Add the following to the connector configuration:

        "transforms": "outbox", --> name of the database table
        "transforms.outbox.type": "com.cloudera.kafka.connect.debezium.transformer.CustomDebeziumTopicTransformer",

