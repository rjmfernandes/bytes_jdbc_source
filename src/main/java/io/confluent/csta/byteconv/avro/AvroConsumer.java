package io.confluent.csta.byteconv.avro;

import java.io.UnsupportedEncodingException;


public class AvroConsumer {

    public static void main(String[] args) throws UnsupportedEncodingException {
        GeneralAvroConsumer.readTopic("postgres2-mytable2");
    }
}