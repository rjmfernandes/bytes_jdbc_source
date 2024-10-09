package io.confluent.csta.byteconv.transforms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class StringToBytesEncondeConverter {
    private static final Logger log = LoggerFactory.getLogger(StringToBytesEncondeConverter.class);

    public static byte[] convertString(String s, String sourceChartsetName, String targetCharsetName)
            throws UnsupportedEncodingException {
        byte[] sourceBytes = s.getBytes(sourceChartsetName);
        log.info("Source Binary:<start>" + new String(sourceBytes,sourceChartsetName) + "<end>");

        byte[] targetBytes = s.getBytes(targetCharsetName);
        log.info("Target Binary:<start>" + new String(targetBytes,targetCharsetName) + "<end>");

        return targetBytes;

    }
}
