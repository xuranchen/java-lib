package com.wavefront.ingester;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import java.util.function.Function;

/**
 * Tests for {@link SpanLogsSerializer}.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class SpanLogsSerializerTest {

  private Function<SpanLogs, String> serializer = new SpanLogsSerializer();

  @Test
  public void testSpanLogsToString() {
    SpanLogs spanLogs = SpanLogs.newBuilder()
        .setCustomer("dummy")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setLogs(ImmutableList.of(new SpanLog(1554363517965L, ImmutableMap.of("event", "error",
            "error.kind", "exception"))))
        .build();
    assertEquals("{" +
        "\"customer\":\"dummy\"," +
        "\"traceId\":\"d5355bf7-fc8d-48d1-b761-75b170f396e0\"," +
        "\"spanId\":\"4217104a-690d-4927-baff-d9aa779414c2\"," +
        "\"spanSecondaryId\":null," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]," +
        "\"span\":null" +
        "}",
        serializer.apply(spanLogs));
  }

  @Test
  public void testSpanLogsToStringWithSpanSecondaryId() {
    SpanLogs spanLogs = SpanLogs.newBuilder()
        .setCustomer("dummy")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setSpanSecondaryId("server")
        .setLogs(ImmutableList.of(new SpanLog(1554363517965L, ImmutableMap.of("event", "error",
            "error.kind", "exception"))))
        .build();
    assertEquals("{" +
            "\"customer\":\"dummy\"," +
            "\"traceId\":\"d5355bf7-fc8d-48d1-b761-75b170f396e0\"," +
            "\"spanId\":\"4217104a-690d-4927-baff-d9aa779414c2\"," +
            "\"spanSecondaryId\":\"server\"," +
            "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]," +
            "\"span\":null" +
            "}",
        serializer.apply(spanLogs));
  }

  @Test
  public void testSpanLogsToStringWithSpan() {
    SpanLogs spanLogs = SpanLogs.newBuilder()
        .setCustomer("dummy")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setLogs(ImmutableList.of(new SpanLog(1554363517965L, ImmutableMap.of("event", "error",
            "error.kind", "exception"))))
        .setSpan("\"testSpanName\" \"source\"=\"spanSource\" " +
            "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
            "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" " +
            "\"tagkey1\"=\"tagvalue1\" \"t2\"=\"v2\" 1532012145123 1532012146234")
        .build();
    assertEquals("{" +
        "\"customer\":\"dummy\"," +
        "\"traceId\":\"d5355bf7-fc8d-48d1-b761-75b170f396e0\"," +
        "\"spanId\":\"4217104a-690d-4927-baff-d9aa779414c2\"," +
        "\"spanSecondaryId\":null," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]," +
        "\"span\":\"\\\"testSpanName\\\" \\\"source\\\"=\\\"spanSource\\\" " +
          "\\\"spanId\\\"=\\\"4217104a-690d-4927-baff-d9aa779414c2\\\" " +
          "\\\"traceId\\\"=\\\"d5355bf7-fc8d-48d1-b761-75b170f396e0\\\" " +
          "\\\"tagkey1\\\"=\\\"tagvalue1\\\" \\\"t2\\\"=\\\"v2\\\" 1532012145123 1532012146234\"" +
        "}",
        serializer.apply(spanLogs));
  }
}
