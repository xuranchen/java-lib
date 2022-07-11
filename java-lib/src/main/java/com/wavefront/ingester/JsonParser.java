package com.wavefront.ingester;

import wavefront.report.Annotation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class JsonParser {
    private final Map<String, Object> input;
    private final String DELIMITER = "_";
    private final List<String> ignoreFlatten;

    public JsonParser(@Nonnull Map<String, Object> input, List<String> ignoreFlatten) {
        this.input = input;
        this.ignoreFlatten = ignoreFlatten;
    }

    public void flattenJson(List<Annotation> annotations) {
        for (Map.Entry<String, Object> tagKV : input.entrySet()) {
            recursiveHelper(tagKV.getKey(), tagKV.getValue(), annotations);
        }
    }

    public void recursiveHelper(String key, Object value, List<Annotation> annotations) {
        if (value == null) {
            annotations.add(Annotation.newBuilder().setKey(key).setValue("null").build());
        } else if (ignoreFlatten.contains(key) || value instanceof String) {
            annotations.add(Annotation.newBuilder().setKey(key).setValue(value.toString()).build());
        } else if (value instanceof Map<?, ?>) {
            for (Map.Entry<?, ?> tagKV : ((Map<?, ?>)value).entrySet()) {
                recursiveHelper(concatonate(key, tagKV.getKey().toString()), tagKV.getValue(), annotations);
            }
        } else if (value instanceof List) {
            for (int i = 0; i < ((List<?>) value).size(); i++) {
                recursiveHelper(concatonate(key, String.valueOf(i)), ((List<?>) value).get(i), annotations);
            }
        } else {
            annotations.add(Annotation.newBuilder().setKey(key).setValue(value.toString()).build());
        }
    }

    private String concatonate(String s1, String s2) {
        return s1 + DELIMITER + s2;
    }
}
