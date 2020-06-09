package com.wavefront.ingester;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import wavefront.report.SpanLogs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for SpanLogsDecoder
 *
 * @author zhanghan@vmware.com
 */
public class SpanLogsDecoderTest {

  private final SpanLogsDecoder decoder = new SpanLogsDecoder();
  private final ObjectMapper jsonParser = new ObjectMapper();

  @Test
  public void testDecodeWithoutSpanSecondaryId() throws IOException {
    List<SpanLogs> out = new ArrayList<>();
    String msg = "{" +
        "\"customer\":\"default\"," +
        "\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
        "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]}";

    decoder.decode(jsonParser.readTree(msg), out, "testCustomer");
    assertEquals(1, out.size());
    assertEquals("testCustomer", out.get(0).getCustomer());
    assertEquals("7b3bf470-9456-11e8-9eb6-529269fb1459", out.get(0).getTraceId());
    assertEquals("0313bafe-9457-11e8-9eb6-529269fb1459", out.get(0).getSpanId());
    assertNull(out.get(0).getSpanSecondaryId());
    assertEquals(1, out.get(0).getLogs().size());
    assertEquals(1554363517965L, out.get(0).getLogs().get(0).getTimestamp());
    assertEquals(2, out.get(0).getLogs().get(0).getFields().size());
    assertEquals("error", out.get(0).getLogs().get(0).getFields().get("event"));
    assertEquals("exception", out.get(0).getLogs().get(0).getFields().get("error.kind"));
    assertNull(out.get(0).getSpan());
  }

  @Test
  public void testDecodeWithSpanSecondaryId() throws IOException {
    List<SpanLogs> out = new ArrayList<>();
    String msg = "{" +
        "\"customer\":\"default\"," +
        "\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
        "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
        "\"spanSecondaryId\":\"server\"," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]}";

    decoder.decode(jsonParser.readTree(msg), out, "testCustomer");
    assertEquals(1, out.size());
    assertEquals("testCustomer", out.get(0).getCustomer());
    assertEquals("7b3bf470-9456-11e8-9eb6-529269fb1459", out.get(0).getTraceId());
    assertEquals("0313bafe-9457-11e8-9eb6-529269fb1459", out.get(0).getSpanId());
    assertEquals("server", out.get(0).getSpanSecondaryId());
    assertEquals(1, out.get(0).getLogs().size());
    assertEquals(1554363517965L, out.get(0).getLogs().get(0).getTimestamp());
    assertEquals(2, out.get(0).getLogs().get(0).getFields().size());
    assertEquals("error", out.get(0).getLogs().get(0).getFields().get("event"));
    assertEquals("exception", out.get(0).getLogs().get(0).getFields().get("error.kind"));
    assertNull(out.get(0).getSpan());
  }

  @Test
  public void testDecodeWithSpan() throws IOException {
    List<SpanLogs> out = new ArrayList<>();
    String msg = "{" +
        "\"customer\":\"default\"," +
        "\"traceId\":\"7b3bf470-9456-11e8-9eb6-529269fb1459\"," +
        "\"spanId\":\"0313bafe-9457-11e8-9eb6-529269fb1459\"," +
        "\"logs\":[{\"timestamp\":1554363517965,\"fields\":{\"event\":\"error\",\"error.kind\":\"exception\"}}]," +
        "\"span\":\"\\\"testSpanName\\\" \\\"source\\\"=\\\"spanSource\\\" " +
          "\\\"spanId\\\"=\\\"4217104a-690d-4927-baff-d9aa779414c2\\\" " +
          "\\\"traceId\\\"=\\\"d5355bf7-fc8d-48d1-b761-75b170f396e0\\\" " +
          "\\\"tagkey1\\\"=\\\"tagvalue1\\\" \\\"t2\\\"=\\\"v2\\\" 1532012145123 1532012146234\"" +
        "}";

    decoder.decode(jsonParser.readTree(msg), out, "testCustomer");
    assertEquals(1, out.size());
    assertEquals("testCustomer", out.get(0).getCustomer());
    assertEquals("7b3bf470-9456-11e8-9eb6-529269fb1459", out.get(0).getTraceId());
    assertEquals("0313bafe-9457-11e8-9eb6-529269fb1459", out.get(0).getSpanId());
    assertNull(out.get(0).getSpanSecondaryId());
    assertEquals(1, out.get(0).getLogs().size());
    assertEquals(1554363517965L, out.get(0).getLogs().get(0).getTimestamp());
    assertEquals(2, out.get(0).getLogs().get(0).getFields().size());
    assertEquals("error", out.get(0).getLogs().get(0).getFields().get("event"));
    assertEquals("exception", out.get(0).getLogs().get(0).getFields().get("error.kind"));
    assertEquals("\"testSpanName\" \"source\"=\"spanSource\" " +
        "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
        "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" " +
        "\"tagkey1\"=\"tagvalue1\" \"t2\"=\"v2\" 1532012145123 1532012146234",
        out.get(0).getSpan());
  }
}
