package com.cloudera.kafka.connect.debezium.transformer;


import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Custom SMT class to route messages to the correct topic
 * @param <R>
 */
public class CustomDebeziumTopicTransformer<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public R apply(R record) {
        // Ignore tombstones
        if (record.value() == null) {
            return record;
        }

        // Extract the record from the debezium message
        Struct struct = (Struct) record.value();
        String op = struct.getString("op");

        // Ignore delete, read and update operations
        if (op.equals("d") || op.equals("r") || op.equals("u")) {
            return null;
        }
        // Only transform in case of a create operation
        else if (op.equals("c")) {
            Struct after = struct.getStruct("after");

            // Get the details
            String key = after.getString("uuid");
            String eventType = after.getString("event_type");
            String topic = after.getString("aggregate_type").toLowerCase() + "Events";
            Long createdOn = after.getInt64("created_on");
            String payload = after.getString("payload");

            // Create the schema
            Schema valueSchema = SchemaBuilder.struct()
                    .field("eventType", after.schema().field("event_type").schema())
                    .field("createdOn", after.schema().field("created_on").schema())
                    .field("payload", after.schema().field("payload").schema())
                    .build();

            // Create the values
            Struct value = new Struct(valueSchema)
                    .put("eventType", eventType)
                    .put("createdOn", createdOn)
                    .put("payload", payload);

            // Add required headers
            Headers headers = record.headers();
            headers.addString("eventId", key);

            return record.newRecord(topic, null, Schema.STRING_SCHEMA, key, valueSchema, value,
                    record.timestamp(), headers);
        } else {
            throw new IllegalArgumentException("The record contains an unexpected operation type: " + record);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
