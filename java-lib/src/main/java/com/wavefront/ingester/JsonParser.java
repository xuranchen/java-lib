package com.wavefront.ingester;

import wavefront.report.Annotation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * Parses Json inputs to parsable format
 */
public class JsonParser {
    private final Map<String, Object> input;
    private final String UNDERSCORE = "_";
    private final List<String> ignoreFlatten;

    /***
     * @param ignoreFlatten list of strings that will not be flattened
     */
    public JsonParser(@Nonnull Map<String, Object> input, List<String> ignoreFlatten) {
        this.input = input;
        this.ignoreFlatten = ignoreFlatten;
    }

    /**
     * flattens nested json
     * a : {b : x} -> a_b = x
     * a:[x, y] -> a_0 = x, a_1 = y
     * @param annotations annotations list to insert the final objects
     */
    public void flattenJson(List<Annotation> annotations) {
        for (Map.Entry<String, Object> tagKV : input.entrySet()) {
            flattenJsonInternal(tagKV.getKey(), tagKV.getValue(), annotations);
        }
    }

    private void flattenJsonInternal(String key, Object value, List<Annotation> annotations) {
        if (value == null) {
            annotations.add(Annotation.newBuilder().setKey(labelReplace(key)).setValue("null").build());
        } else if (ignoreFlatten.contains(key) || value instanceof String) {
            annotations.add(Annotation.newBuilder().setKey(labelReplace(key)).setValue(value.toString()).build());
        } else if (value instanceof Map<?, ?>) {
            for (Map.Entry<?, ?> tagKV : ((Map<?, ?>)value).entrySet()) {
                flattenJsonInternal(concatonate(key, tagKV.getKey().toString()), tagKV.getValue(), annotations);
            }
        } else if (value instanceof List) {
            for (int i = 0; i < ((List<?>) value).size(); i++) {
                flattenJsonInternal(concatonate(key, String.valueOf(i)), ((List<?>) value).get(i), annotations);
            }
        } else {
            annotations.add(Annotation.newBuilder().setKey(labelReplace(key)).setValue(value.toString()).build());
        }
    }

    private String concatonate(String s1, String s2) {
        return s1 + UNDERSCORE + s2;
    }
    private String labelReplace(String label) {
        String intermediate = label.replace('-', '_');
        return intermediate.replace('.', '_');
    }
}
