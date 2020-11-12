package com.github.coderlong.util;

import static com.fasterxml.jackson.databind.type.TypeFactory.defaultInstance;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author rensailong
 * Created on 2020-11-05
 */
public class JsonUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String EMPTY_JSON = "{}";

    private static final String EMPTY_ARRAY_JSON = "[]";

    public static final String JSON_PARSER_PREFIX = "{";

    /**
     * Json TreeMode判断
     */
    public static boolean isJson(String jsonStr) {
        if (jsonStr == null) {
            return false;
        }
        try {
            OBJECT_MAPPER.readTree(jsonStr);
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    public static String toJson(Object object) {
        String jsonStr = "";
        try {
            jsonStr = OBJECT_MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            return EMPTY_JSON;
        }
        return jsonStr;
    }

    public static <T> T fromJson(String jsonStr, Class<T> typeClass) {
        return fromJson(jsonStr.getBytes(), typeClass);
    }

    public static <T> T fromJson(byte[] bytes, Class<T> typeClass) {
        try {
            return OBJECT_MAPPER.readValue(bytes, typeClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static Map<String, Object> fromJson(String string) {
        return fromJSON(string, Map.class, String.class, Object.class);
    }

    public static Map<String, Object> fromJson(byte[] bytes) {
        return fromJSON(bytes, Map.class, String.class, Object.class);
    }

    public static <K, V, T extends Map<K, V>> T fromJSON(String json, Class<? extends Map> mapType,
            Class<K> keyType, Class<V> valueType) {
        if (StringUtils.isEmpty(json)) {
            json = EMPTY_JSON;
        }
        try {
            return OBJECT_MAPPER.readValue(json,
                    defaultInstance().constructMapType(mapType, keyType, valueType));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <K, V, T extends Map<K, V>> T fromJSON(byte[] bytes, Class<? extends Map> mapType,
            Class<K> keyType, Class<V> valueType) {
        try {
            return OBJECT_MAPPER.readValue(bytes,
                    defaultInstance().constructMapType(mapType, keyType, valueType));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
