package io.confluent.csta.byteconv.avro;

import java.io.UnsupportedEncodingException;



public class AvroConsumer2 {

    public static void main(String[] args) throws UnsupportedEncodingException {
        GeneralAvroConsumer.readTopic("oracle-mytable");
    }


}