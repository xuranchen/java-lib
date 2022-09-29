package com.wavefront.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import junit.framework.TestCase;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class LogTest {

    @Test
    public void testSerialize() throws JsonProcessingException {
        Log log1 = new Log(new ReportLog(1234567L, "oops", "myHost", Collections.singletonList(
                new Annotation("key1", "value1")
        )));
        ObjectMapper mapper = new ObjectMapper();
        String jsonResult = mapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(log1);
        Log out = mapper.readValue(jsonResult, Log.class);
        assertEquals(log1, out);
    }

    @Test
    public void testToString_NoAnnotations() {

        Log log1 = new Log(new ReportLog(1234567L, "oops", "myHost", new ArrayList<>()));
        String expected = "{\"timestamp\":1234567, \"text\":\"oops\", \"source\":\"myHost\"}";
        assertEquals(expected, log1.toString());
    }

    @Test
    public void testToString_WithAnnotations() {

        Log log1 = new Log(new ReportLog(1234567L, "oops", "myHost", Collections.singletonList(
                new Annotation("key1", "value1")
        )));

        String expected = "{\"key1\":\"value1\", \"timestamp\":1234567, \"text\":\"oops\", \"source\":\"myHost\"}";
        assertEquals(expected, log1.toString());
    }
}