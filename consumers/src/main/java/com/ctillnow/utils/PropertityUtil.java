package com.ctillnow.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/23 14:29
 * 4
 */
public class PropertityUtil {
    private static Properties properties = null;
    public static Properties getPropertity(){
        InputStream systemResourceAsStream = ClassLoader.getSystemResourceAsStream("kafka.properties");
        try {
            properties = new Properties();
            properties.load(systemResourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return properties;
    }
}
