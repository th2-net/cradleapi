package com.exactpro.cradle.utils;

public class TestUtils {
    public static String generateUnicodeString(int start, int size) {
        StringBuilder generated = new StringBuilder();
        for(int i = 0;i < size;i++){
            generated.append(Character.toString(start++));
        }
        return generated.toString();
    }
}
