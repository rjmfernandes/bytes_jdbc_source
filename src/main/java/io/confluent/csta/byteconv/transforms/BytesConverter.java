package io.confluent.csta.byteconv.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class BytesConverter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(BytesConverter.class);
    public static final String DEFAULT_SOURCE_CHARSET = "ISO-8859-1";
    public static final String DEFAULT_TARGET_CHARSET = "IBM285";

    private interface ConfigName {
        String FIELD_NAME = "field.name";
        String SOURCE_CHARSET = "source.charset";
        String TARGET_CHARSET = "target.charset";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BytesConverter.ConfigName.FIELD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "field to convert")
            .define(BytesConverter.ConfigName.SOURCE_CHARSET, ConfigDef.Type.STRING, DEFAULT_SOURCE_CHARSET,
                    ConfigDef.Importance.HIGH,
                    "charset of original field, default is ISO-8859-1")
            .define(BytesConverter.ConfigName.TARGET_CHARSET, ConfigDef.Type.STRING, DEFAULT_TARGET_CHARSET,
                    ConfigDef.Importance.HIGH,
                    "charset of converted field, default is IBM285");

    private static final String PURPOSE =
            "convert a string field into bytes using specified charsets for source and target";

    private String fieldName;
    private String sourceCharset;
    private String targetCharset;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(BytesConverter.ConfigName.FIELD_NAME);
        sourceCharset = config.getString(BytesConverter.ConfigName.SOURCE_CHARSET);
        targetCharset = config.getString(BytesConverter.ConfigName.TARGET_CHARSET);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (value.get(fieldName) instanceof String) try {
            updatedValue.replace(fieldName, StringToBytesEncondeConverter.
                    convertString((String) value.get(fieldName), sourceCharset, targetCharset));
        } catch (UnsupportedEncodingException e) {
            log.error("Error converting field value: " + value.get(fieldName) + " with source charset " + sourceCharset +
                    " and target charset " + targetCharset);
            throw new RuntimeException(e);
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (field.name().equals(fieldName)) {
                try {
                    updatedValue.put(fieldName, StringToBytesEncondeConverter.
                            convertString((String) value.get(fieldName), sourceCharset, targetCharset));
                } catch (UnsupportedEncodingException e) {
                    log.error("Error converting field value: " + value.get(fieldName) + " with source charset " + sourceCharset +
                            " and target charset " + targetCharset);
                    throw new RuntimeException(e);
                }
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }


        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (field.name().equals(fieldName)) {
                builder.field(field.name(), Schema.OPTIONAL_BYTES_SCHEMA);
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        Schema newschema = builder.build();

        for(Field field : newschema.fields()) {
            log.info("Field name: " + field.name() + " Field schema: " + field.schema());
        }

        return newschema;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends BytesConverter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends BytesConverter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
