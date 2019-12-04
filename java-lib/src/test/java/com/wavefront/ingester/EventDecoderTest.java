package com.wavefront.ingester;

import org.junit.Assert;
import org.junit.Test;
import wavefront.report.ReportEvent;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author vasily@wavefront.com
 */
public class EventDecoderTest {

  private EventDecoder decoder = new EventDecoder();

  private static long startTs = 1569423200123L;
  private static long endTs = 1569423260123L;

  @Test(expected = RuntimeException.class)
  public void testDecodeEventWithIncorrectCaseThrows() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@event 1569423200123 1569423260123 \"Event name for testing\"", out);
    Assert.fail("Invalid event literal didn't throw");
  }

  @Test(expected = RuntimeException.class)
  public void testDecodeEventWithIncorrectLiteralThrows() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("Event 1569423200123 1569423260123 \"Event name for testing\"", out);
    Assert.fail("Invalid event literal didn't throw");
  }

  @Test(expected = RuntimeException.class)
  public void testDecodeEventWithIncorrectNameThrows() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 1569423260123 \"Event name for testing\"",
        out);
    Assert.fail("Invalid event name didn't throw");
  }

  @Test(expected = RuntimeException.class)
  public void testDecodeEventWithIncorrectKvPairsThrows() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 \"Event name for testing\" invalid kv pairs", out);
    Assert.fail("Invalid event kv pairs didn't throw");
  }

  @Test
  public void testDecodeBasicEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "type=deployment-event host=app1 severity=INFO host=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \n end of description\" " +
        "tag=eventtag2 host=app4 somerandomannotation=value", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(endTs, out.get(0).getEndTime().longValue());
    assertEquals("Event name for testing", out.get(0).getName());
    assertEquals(4, out.get(0).getHosts().size());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals(2, out.get(0).getTags().size());
    assertArrayEquals(new String[] {"eventtag1", "eventtag2"}, out.get(0).getTags().toArray());
    assertEquals(4, out.get(0).getAnnotations().size());
    assertNull(out.get(0).getDimensions());
    assertEquals("Really long description with a line break here: \n end of description",
        out.get(0).getAnnotations().get("description"));
    assertEquals("deployment-event", out.get(0).getAnnotations().get("type"));
    assertEquals("INFO", out.get(0).getAnnotations().get("severity"));
    assertEquals("value", out.get(0).getAnnotations().get("somerandomannotation"));
  }

  @Test
  public void testDecodeBasicEventWithDimensions() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "type=deployment-event multi=foo host=app1 multi=bar multi2=foo2 severity=INFO " +
        "multi2=bar2 host=app2 multi=baz", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(endTs, out.get(0).getEndTime().longValue());
    assertEquals("Event name for testing", out.get(0).getName());
    assertEquals(2, out.get(0).getHosts().size());
    assertArrayEquals(new String[] {"app1", "app2"}, out.get(0).getHosts().toArray());
    assertEquals(2, out.get(0).getAnnotations().size());
    assertEquals("deployment-event", out.get(0).getAnnotations().get("type"));
    assertEquals("INFO", out.get(0).getAnnotations().get("severity"));
    assertEquals(2, out.get(0).getAnnotations().size());
    assertArrayEquals(new String[] {"foo", "bar", "baz"}, out.get(0).getDimensions().get("multi").toArray());
    assertArrayEquals(new String[] {"foo2", "bar2"}, out.get(0).getDimensions().get("multi2").toArray());
  }

  @Test
  public void testDecodeBasicEventWithQuotedIdentifiers() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "\"type\"=\"deployment-event\" host=\"app1\" severity=INFO \"host\"=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \n end of description\" " +
        "\"tag\"=\"eventtag2\" host=\"app4\" \"somerandomannotation\"=value", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(endTs, out.get(0).getEndTime().longValue());
    assertEquals("Event name for testing", out.get(0).getName());
    assertEquals(4, out.get(0).getHosts().size());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals(2, out.get(0).getTags().size());
    assertArrayEquals(new String[] {"eventtag1", "eventtag2"}, out.get(0).getTags().toArray());
    assertEquals(4, out.get(0).getAnnotations().size());
    assertEquals("Really long description with a line break here: \n end of description",
        out.get(0).getAnnotations().get("description"));
    assertEquals("deployment-event", out.get(0).getAnnotations().get("type"));
    assertEquals("INFO", out.get(0).getAnnotations().get("severity"));
    assertEquals("value", out.get(0).getAnnotations().get("somerandomannotation"));
  }

  @Test
  public void testDecodeBasicInstantEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 \"Event name for testing\" " +
        "type=deployment-event host=app1 severity=INFO host=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \n end of description\" " +
        "tag=eventtag2 host=app4 somerandomannotation=value", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(startTs + 1, out.get(0).getEndTime().longValue());
    assertEquals("Event name for testing", out.get(0).getName());
    assertEquals(4, out.get(0).getHosts().size());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals(2, out.get(0).getTags().size());
    assertArrayEquals(new String[] {"eventtag1", "eventtag2"}, out.get(0).getTags().toArray());
    assertEquals(4, out.get(0).getAnnotations().size());
    assertEquals("Really long description with a line break here: \n end of description",
        out.get(0).getAnnotations().get("description"));
    assertEquals("deployment-event", out.get(0).getAnnotations().get("type"));
    assertEquals("INFO", out.get(0).getAnnotations().get("severity"));
    assertEquals("value", out.get(0).getAnnotations().get("somerandomannotation"));
  }

  @Test
  public void testDecodeMinimumViableEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\"", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(endTs, out.get(0).getEndTime().longValue());
    assertEquals("Event name for testing", out.get(0).getName());
    assertEquals(0, out.get(0).getHosts().size());
    assertNull(out.get(0).getTags());
    assertEquals(0, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeMinimumViableEventWithUnquotedName() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 Event_name_for_testing", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(endTs, out.get(0).getEndTime().longValue());
    assertEquals("Event_name_for_testing", out.get(0).getName());
    assertEquals(0, out.get(0).getHosts().size());
    assertNull(out.get(0).getTags());
    assertEquals(0, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeMinimumViableInstantEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 \"Event name for testing\"", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartTime().longValue());
    assertEquals(startTs + 1, out.get(0).getEndTime().longValue());
    assertEquals("Event name for testing", out.get(0).getName());
    assertEquals(0, out.get(0).getHosts().size());
    assertNull(out.get(0).getTags());
    assertEquals(0, out.get(0).getAnnotations().size());
  }
}
