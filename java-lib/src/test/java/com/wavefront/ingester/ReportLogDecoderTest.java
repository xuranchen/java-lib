package com.wavefront.ingester;

import com.wavefront.common.Clock;
import org.junit.Assert;
import org.junit.Test;
import wavefront.report.ReportLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;


import static junit.framework.TestCase.*;

public class ReportLogDecoderTest {
    private final String defaultHost = "unitTest";
    private final Supplier<String> defaultHostSupplier = () -> defaultHost;

    // Tests an empty message with no custom formatting with both an existing and a null default HostSupplier
    @Test
    public void testEmptyMessage() {
        // uses default Host Supplier
        ReportLogDecoder decoder = new ReportLogDecoder(defaultHostSupplier, null, null, null);
        List<ReportLog> out = new ArrayList<>();

        decoder.decode("{}", out, "unitTestCustomer", null);
        assertEquals(out.size(), 1);
        ReportLog log = out.get(0);
        assertEquals(log.getMessage(), "");
        assertEquals(log.getHost(), defaultHost);
        assertEquals(log.getAnnotations().size(), 0);

        // no Default Host Supplier
        decoder = new ReportLogDecoder(null, null, null, null);
        out = new ArrayList<>();

        decoder.decode("{}", out, "unitTestCustomer", null);
        assertEquals(out.size(), 1);
        log = out.get(0);
        assertEquals(log.getMessage(), "");
        assertEquals(log.getHost(), "unknown");
        assertEquals(log.getAnnotations().size(), 0);
    }

    // Tests messages that are not successful
    @Test
    public void testInvalidMessage() {
        // The input message is not in JSON format
        ReportLogDecoder decoder = new ReportLogDecoder(defaultHostSupplier, null, null, null);
        List<ReportLog> out = new ArrayList<>();

        decoder.decode("", out, "unitTestCustomer", null);
        assertEquals(out.size(), 1);
        ReportLog log = out.get(0);
        assertNull(log);

        // The timestamp is not numerical
        decoder = new ReportLogDecoder(defaultHostSupplier, null, null, null);
        out = new ArrayList<>();

        decoder.decode("{\"timestamp\": \"asdf\"}", out, "unitTestCustomer", null);
        assertEquals(out.size(), 1);
        log = out.get(0);
        Assert.assertNotEquals(log.getTimestamp(), "asdf");
    }

    // Tests a basic message with no custom formatting
    @Test
    public void testBasicMessage() {
        ReportLogDecoder decoder = new ReportLogDecoder(defaultHostSupplier, null, null, null);
        List<ReportLog> out = new ArrayList<>();

        long curTime = Clock.now();
        String jsonStr = "{\n" +
                "   \"message\": \"a log message\",\n" +
                "   \"source\": \"my unit test\",\n" +
                "   \"timestamp\": \"" + curTime + "\",\n" +
                "   \"extraTag1\": \"extraValue1\",\n" +
                "   \"extraTag2\": \"extraValue2\"\n" +
                "}";
        decoder.decode(jsonStr, out, "unitTestCustomer", null);
        assertEquals(out.size(), 1);
        ReportLog log = out.get(0);
        assertEquals(log.getMessage(), "a log message");
        assertEquals(log.getHost(), "my unit test");
        assertEquals(log.getTimestamp(), curTime);
        assertEquals(log.getAnnotations().size(), 2);
        assertEquals(log.getAnnotations().get(0).getKey(), "extraTag1");
        assertEquals(log.getAnnotations().get(1).getKey(), "extraTag2");
        assertEquals(log.getAnnotations().get(0).getValue(), "extraValue1");
        assertEquals(log.getAnnotations().get(1).getValue(), "extraValue2");
    }

    // Tests custom formatting
    @Test
    public void testCustomMessage() {
        ReportLogDecoder decoder = new ReportLogDecoder(defaultHostSupplier, Collections.singletonList("customHost"), Collections.singletonList("customTimestamp"), Collections.singletonList("customMessage"));
        List<ReportLog> out = new ArrayList<>();

        long curTime = Clock.now();
        String jsonStr = "{\n" +
                "   \"customMessage\": \"a log message\",\n" +
                "   \"customHost\": \"my unit test\",\n" +
                "   \"customTimestamp\": \"" + curTime + "\"\n" +
                "}";
        decoder.decode(jsonStr, out, "unitTestCustomer", null);
        assertEquals(out.size(), 1);
        ReportLog log = out.get(0);
        assertEquals(log.getMessage(), "a log message");
        assertEquals(log.getHost(), "my unit test");
        assertEquals(log.getTimestamp(), curTime);
        assertEquals(log.getAnnotations().size(), 3);
        assertEquals(log.getAnnotations().get(0).getKey(), "customMessage");
        assertEquals(log.getAnnotations().get(1).getKey(), "customHost");
        assertEquals(log.getAnnotations().get(2).getKey(), "customTimestamp");
        assertEquals(log.getAnnotations().get(0).getValue(), "a log message");
        assertEquals(log.getAnnotations().get(1).getValue(), "my unit test");
        assertEquals(log.getAnnotations().get(2).getValue(), String.valueOf(curTime));
    }
}