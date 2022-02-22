package com.chickenrunfanclub.shared;

import org.apache.commons.codec.digest.DigestUtils;

public class Hasher {
    public static String hash(String key) {
        return DigestUtils.md5Hex(key).toUpperCase();
    }
}
