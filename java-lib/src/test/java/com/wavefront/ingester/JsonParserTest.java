package com.wavefront.ingester;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.Annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonParserTest {
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        this.objectMapper = new ObjectMapper();
    }

    @Test
    public void testFlattenJson_noop() {
        String testStr =
                "{" +
                    "\"a\":\"b\"," +
                    "\"c\":\"d\"" +
                "}";
        try {
            Map<String, Object> tagMap = objectMapper.readValue(testStr, new TypeReference<Map<String,Object>>(){});
            JsonParser jsonParser = new JsonParser(tagMap, AbstractIngesterFormatter.getDefaultLogMessageKeys());
            List<Annotation> annotations = new ArrayList<>();
            jsonParser.flattenJson(annotations);
            assertEquals(annotations.size(), 2);
            assertEquals(annotations.get(0), Annotation.newBuilder().setKey("a").setValue("b").build());
            assertEquals(annotations.get(1), Annotation.newBuilder().setKey("c").setValue("d").build());
        } catch (JsonProcessingException e) {
            fail("error parsing json input");
        }
    }

    @Test
    public void testFlattenJson_flattenMap() {
        String testStr =
                "{" +
                        "\"a\":\"b\"," +
                        "\"c\": {\"d\":\"e\"," +
                            "\"f\":\"g\"}" +
                        "}";
        try {
            Map<String, Object> tagMap = objectMapper.readValue(testStr, new TypeReference<Map<String,Object>>(){});
            JsonParser jsonParser = new JsonParser(tagMap, AbstractIngesterFormatter.getDefaultLogMessageKeys());
            List<Annotation> annotations = new ArrayList<>();
            jsonParser.flattenJson(annotations);
            assertEquals(annotations.size(), 3);
            assertEquals(annotations.get(0), Annotation.newBuilder().setKey("a").setValue("b").build());
            assertEquals(annotations.get(1), Annotation.newBuilder().setKey("c_d").setValue("e").build());
            assertEquals(annotations.get(2), Annotation.newBuilder().setKey("c_f").setValue("g").build());
        } catch (JsonProcessingException e) {
            fail("error parsing json input");
        }
    }

    @Test
    public void testFlattenJson_flattenList() {
        String testStr =
                "{" +
                        "\"a\":\"b\"," +
                        "\"c\": [\"d\", \"e\"," +
                        "{\"f\":\"g\"}]" +
                        "}";
        try {
            Map<String, Object> tagMap = objectMapper.readValue(testStr, new TypeReference<Map<String,Object>>(){});
            JsonParser jsonParser = new JsonParser(tagMap, AbstractIngesterFormatter.getDefaultLogMessageKeys());
            List<Annotation> annotations = new ArrayList<>();
            jsonParser.flattenJson(annotations);
            assertEquals(annotations.size(), 4);
            assertEquals(annotations.get(0), Annotation.newBuilder().setKey("a").setValue("b").build());
            assertEquals(annotations.get(1), Annotation.newBuilder().setKey("c_0").setValue("d").build());
            assertEquals(annotations.get(2), Annotation.newBuilder().setKey("c_1").setValue("e").build());
            assertEquals(annotations.get(3), Annotation.newBuilder().setKey("c_2_f").setValue("g").build());
        } catch (JsonProcessingException e) {
            fail("error parsing json input");
        }
    }

    @Test
    public void testFlattenJson_ignoreTags() {
        String testStr =
                "{" +
                        "\"a\":\"b\"," +
                        "\"message\": {\"d\":\"e\"," +
                        "\"f\":\"g\"}" +
                        "}";
        try {
            Map<String, Object> tagMap = objectMapper.readValue(testStr, new TypeReference<Map<String,Object>>(){});
            JsonParser jsonParser = new JsonParser(tagMap, AbstractIngesterFormatter.getDefaultLogMessageKeys());
            List<Annotation> annotations = new ArrayList<>();
            jsonParser.flattenJson(annotations);
            assertEquals(annotations.size(), 2);
            assertEquals(annotations.get(0), Annotation.newBuilder().setKey("a").setValue("b").build());
            assertEquals(annotations.get(1), Annotation.newBuilder().setKey("message").setValue("{d=e, f=g}").build());
        } catch (JsonProcessingException e) {
            fail("error parsing json input");
        }
    }

    @Test
    public void testFlattenJson_replace() {
        String testStr =
                "{" +
                        "\"a.a\":\"b\"," +
                        "\"c-c\":\"d\"," +
                        "\"e/e\":\"f\"" +
                        "}";
        try {
            Map<String, Object> tagMap = objectMapper.readValue(testStr, new TypeReference<Map<String,Object>>(){});
            JsonParser jsonParser = new JsonParser(tagMap, AbstractIngesterFormatter.getDefaultLogMessageKeys());
            List<Annotation> annotations = new ArrayList<>();
            jsonParser.flattenJson(annotations);
            assertEquals(annotations.size(), 3);
            assertEquals(annotations.get(0), Annotation.newBuilder().setKey("a_a").setValue("b").build());
            assertEquals(annotations.get(1), Annotation.newBuilder().setKey("c_c").setValue("d").build());
            assertEquals(annotations.get(2), Annotation.newBuilder().setKey("e_e").setValue("f").build());
        } catch (JsonProcessingException e) {
            fail("error parsing json input");
        }
    }
}