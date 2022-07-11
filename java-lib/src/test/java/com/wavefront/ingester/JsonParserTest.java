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
    String testStr = "{\"log\":\"2022-07-08 15:29:13,140 INFO  [AbstractReportableEntityHandler:printStats] [2878] Points received rate: 0 pps (1 min), 0 pps (5 min), 0 pps (current).\\n\",\"stream\":\"stdout\",\"docker\":{\"container_id\":\"446daf7372ddc630b1a339635c7a18e12199fcbb14af2ea6b52052edff8a9a29\"},\"kubernetes\":{\"container_name\":\"proxy\",\"namespace_name\":\"default\",\"pod_name\":\"hyperlogs-proxy-77476445fb-wnbwp\",\"container_image\":\"wavefronthq/proxy:11.2\",\"container_image_id\":\"docker-pullable://wavefronthq/proxy@sha256:2746f37e087efdd6a8ee32b485b7bd6db9731d03cb31fb9969554ed3fda98435\",\"pod_id\":\"8235fc39-0476-44ec-b8a9-1e9e780455a4\",\"host\":\"minikube\",\"labels\":{\"app\":\"hyperlogs-proxy\",\"pod-template-hash\":\"77476445fb\"}},\"service\":\"myApp\",\"application\":\"myService\",\"timestamp\":\"1657294153140\",\n" +
            "  \"testArray\":[\"a\", \"b\", {\"c\":\"d\", \"e\":\"1\"}]\n" +
            "}";
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
}