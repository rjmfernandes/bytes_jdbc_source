# Bytes on JDBC Source Connector

- [Bytes on JDBC Source Connector](#bytes-on-jdbc-source-connector)
  - [Setup](#setup)
    - [Install JDBC Sink Connector plugin](#install-jdbc-sink-connector-plugin)
    - [Create and populate database table](#create-and-populate-database-table)
  - [JDBC Source Connector handling bytes field](#jdbc-source-connector-handling-bytes-field)
    - [Another encoding](#another-encoding)
  - [JDBC Source Connector handling bytes field from a text field](#jdbc-source-connector-handling-bytes-field-from-a-text-field)
  - [Oracle](#oracle)
    - [Setup](#setup-1)
    - [Create User and Table](#create-user-and-table)
    - [JDBC Source test](#jdbc-source-test)
  - [Cleanup](#cleanup)

## Setup

Start:

```shell
docker compose up -d
```

Monitor logs:

```shell
docker compose logs -f
```

Open Control Center: http://localhost:9021/clusters

### Install JDBC Sink Connector plugin

```shell
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
```

Now we need to restart our connect:

```shell
docker compose restart connect
```

Now if we list our plugins we should see two new ones corresponding to the JDBC connector.

```shell
curl localhost:8083/connector-plugins | jq
```

### Create and populate database table

In Postgres:

```sql
CREATE TABLE mytable (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    data BYTEA
);
```

And we insert some data:

```sql
INSERT INTO mytable (name, data)
VALUES ('example', '\x5468697320697320612062696e61727920737472696e67');
```

We can query the field decoding the binary data:

```sql
SELECT id, name, convert_from(data, 'UTF8') AS decoded_data FROM mytable;
```

## JDBC Source Connector handling bytes field

Now we can define our JDBC Source Connector:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "mytable",
             "mode":"bulk"}'
```

If we read from our topic:

```shell
kafka-avro-console-consumer --topic postgres-mytable --bootstrap-server 127.0.0.1:9092 --property schema.registry.url=http://127.0.0.1:8081 --from-beginning
```

We get:

```json
{"id":1,"name":{"string":"example"},"data":{"bytes":"This is a binary string"}}
```

So the 'UTF8' binary encoding is the one assumed for the conversion and we see the decoded binary when reading the bytes field.

And if we checked the schema:


```shell
curl -s http://localhost:8081/subjects/postgres-mytable-value/versions/latest | jq '.schema|fromjson[]'
```

We see:

```json
[
  {
    "name": "id",
    "type": "int"
  },
  {
    "name": "name",
    "type": [
      "null",
      "string"
    ],
    "default": null
  },
  {
    "name": "data",
    "type": [
      "null",
      "bytes"
    ],
    "default": null
  }
]
```

### Another encoding

If we insert another entry with different encoding:

```sql
INSERT INTO mytable (name,data)
VALUES ('example2',decode('5468697320697320612062696e61727920737472696e67', 'hex'));
```

We can query it:

```sql
SELECT id,name,
       convert_from(data, 'ISO-8859-1') AS decoded_string 
FROM mytable
where id=2;
```

And restarting our connector one would still see:

```json
{
  "id": 2,
  "name": {
    "string": "example2"
  },
  "data": {
    "bytes": "This is a binary string"
  }
}
```

## JDBC Source Connector handling bytes field from a text field

In Postgres:

```sql
CREATE TABLE mytable2 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    data VARCHAR(1000)
);
```

We populate it:

```sql
INSERT INTO mytable2 (name,data)
VALUES ('example','This is a binary string');
```

Now let's create a connector:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source2-postgres/config \
     -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://host.docker.internal:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "password",
             "topic.prefix": "postgres2-",
             "poll.interval.ms" : 3600000,
             "table.whitelist" : "mytable2",
             "mode":"bulk",
             "value.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schemas.enable":"false",
          "key.converter"       : "org.apache.kafka.connect.storage.StringConverter",
          "transforms": "binconvert,SchemaTransform",
            "transforms.binconvert.field.name": "data",
            "transforms.binconvert.type": "io.confluent.csta.timestamp.transforms.BytesConverter$Value",
            "transforms.binconvert.source.charset": "ISO-8859-1",
            "transforms.binconvert.target.charset": "IBM285",
    "transforms.SchemaTransform.type": "org.apache.kafka.connect.transforms.SetSchemaMetadata$Value",
    "transforms.SchemaTransform.schema.name": "io.confluent.csta.byteconv.MyTable"
            }'
```

If we execute:

```shell
kafka-avro-console-consumer --topic postgres2-mytable2 --bootstrap-server 127.0.0.1:9092 --property schema.registry.url=http://127.0.0.1:8081 --from-beginning
```

We should see a message:

```json
{"id":1,"name":{"string":"example"},"data":{"bytes":"ã¢@¢@@¨@¢£"}}
```

But if we run our class `io.confluent.csta.byteconv.avro.AvroConsumer` one can see that it has the proper value with intended encoding IBM285.

```
Key: null
Value: {"id": 1, "name": "example", "data": "ã\u0088\u0089¢@\u0089¢@\u0081@\u0082\u0089\u0095\u0081\u0099¨@¢£\u0099\u0089\u0095\u0087"}
Decoded data: This is a binary string
```

## Oracle 

### Setup

Execute first steps to pull the login and pull the image of oracle database from https://dev.to/docker/how-to-run-oracle-database-in-a-docker-container-using-docker-compose-1c9b.

Or if you are on MAC M1 https://www.simonpcouch.com/blog/2024-03-14-oracle/index.html The compose file here is adapted to this final case.

```shell
cd oracle
docker compose up -d
```

It will take a while to start. Run:

```shell
docker compose logs -f
```

And wait until you see:

```
#########################
DATABASE IS READY TO USE!
#########################
```

### Create User and Table

We have used [Oracle SQL Developer Extension for VSCode](https://marketplace.visualstudio.com/items?itemName=Oracle.sql-developer):

```sql
CREATE USER my_user IDENTIFIED BY mypassword
DEFAULT TABLESPACE users
TEMPORARY TABLESPACE temp
QUOTA UNLIMITED ON users;
GRANT CONNECT, RESOURCE TO my_user;
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE PROCEDURE, CREATE SEQUENCE TO my_user;
```

Let's connect as our user `my_user/mypassword` to the database and create our table and populate it:

```sql
CREATE TABLE mytable (
    id NUMBER PRIMARY KEY,
    name VARCHAR(100),
    data VARCHAR(100)
);
INSERT INTO mytable (id,name,data)
VALUES (0,'example','This is a binary string');
COMMIT;
```

For previous test run:

```sql
SELECT UTL_RAW.CAST_TO_RAW(convert(data,'WE8EBCDIC284','WE8ISO8859P1')) from mytable;
```

You should get something like:

```
E38889A24089A24081408289958199A840A2A399899587
```

(We are using `WE8EBCDIC284` considering it was the encode available on opur oracle instance and not `BSWE8EBCDIC285`.)

### JDBC Source test

Now let's run our connector:

```shell
curl -i -X PUT -H "Accept:application/json" \
     -H "Content-Type: application/json" http://localhost:8083/connectors/my-source-oracle/config \
     -d "{
             \"connector.class\": \"io.confluent.connect.jdbc.JdbcSourceConnector\",
             \"connection.url\": \"jdbc:oracle:thin:@//host.docker.internal:1521/ORCLPDB1\",
             \"connection.user\": \"my_user\",
             \"connection.password\": \"mypassword\",
             \"topic.prefix\": \"oracle-mytable\",
             \"poll.interval.ms\" : 3600000,
             \"query\": \"SELECT ID AS id, NAME as name, UTL_RAW.CAST_TO_RAW(convert(DATA,'WE8EBCDIC284','WE8ISO8859P1')) as data FROM mytable WHERE id IS NOT NULL\",
             \"mode\": \"bulk\",
             \"transforms\": \"RenameIdField, CastIdField, RenameNameField, RenameDataField, SchemaTransform\",
             \"transforms.RenameIdField.type\": \"org.apache.kafka.connect.transforms.ReplaceField\$Value\",
             \"transforms.RenameIdField.renames\": \"ID:id\",
             \"transforms.CastIdField.type\": \"org.apache.kafka.connect.transforms.Cast\$Value\",
             \"transforms.CastIdField.spec\": \"id:int32\",
             \"transforms.RenameNameField.type\": \"org.apache.kafka.connect.transforms.ReplaceField\$Value\",
             \"transforms.RenameNameField.renames\": \"NAME:name\",
             \"transforms.RenameDataField.type\": \"org.apache.kafka.connect.transforms.ReplaceField\$Value\",
             \"transforms.RenameDataField.renames\": \"DATA:data\",
             \"transforms.SchemaTransform.type\": \"org.apache.kafka.connect.transforms.SetSchemaMetadata\$Value\",
            \"transforms.SchemaTransform.schema.name\": \"io.confluent.csta.byteconv.MyTable\",
            \"pk.mode\": \"record_value\",    
            \"pk.fields\": \"id\"  
     }"
```

You should see on your topic a message as:

```json
{
  "id": "\u0000",
  "name": {
    "string": "example"
  },
  "data": {
    "bytes": "ã¢@¢@@¨@¢£"
  }
}
```

Which is quite different than `E38889A24089A24081408289958199A840A2A399899587`

But if we run `io.confluent.csta.byteconv.avro.AvroConsumer2` we can see we get:

```
Key: null
Value: {"id": 0, "name": "example", "data": "ã\u0088\u0089¢@\u0089¢@\u0081@\u0082\u0089\u0095\u0081\u0099¨@¢£\u0099\u0089\u0095\u0087"}
Decoded data: This is a binary string
```

So we are properly capturing the bytes returned by the Oracle query and we are able to decode with the right encoding `IBM285` in our java class.

## Cleanup

```shell
docker compose down -v
cd oracle
docker compose down -v
cd ..
```