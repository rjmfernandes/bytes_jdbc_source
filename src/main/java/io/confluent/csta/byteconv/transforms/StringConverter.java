package io.confluent.csta.byteconv.transforms;

import java.io.UnsupportedEncodingException;

public class StringConverter {

    public static final String SOURCE_CHARSET = "ISO-8859-1";
    public static final String TARGET_CHARSET = "IBM285";
    public static final String SOURCE_STRING = "This is a binary string";

    public static void main(String[] args) throws UnsupportedEncodingException {
        System.out.println("Source String Hex: " + bytesToHex(SOURCE_STRING.getBytes()));
        byte[] convertedBytes = StringToBytesEncondeConverter.convertString(SOURCE_STRING, SOURCE_CHARSET,
                TARGET_CHARSET);
        System.out.println("Converted String Hex: " + bytesToHex(convertedBytes));
        System.out.println("Converted String:<start>" + new String(convertedBytes, TARGET_CHARSET)+ "<end>");
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }
}

