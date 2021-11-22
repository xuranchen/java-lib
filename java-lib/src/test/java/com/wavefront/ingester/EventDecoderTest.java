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
  public void testDecodeBasicOldEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "type=deployment-event host=app1 severity=INFO host=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \n end of description\" " +
        "tag=eventtag2 host=app4 somerandomannotation=value", out);
    assertEquals(1, out.size());
    assertNotNull(out.get(0).getEventId());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(endTs, out.get(0).getEndMillis());
    assertEquals("deployment-event", out.get(0).getGroup());
    assertEquals(4, out.get(0).getHosts().size());
    assertEquals("Really long description with a line break here: \n end of description", out.get(0).getDetails());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals(6, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeBasicNewEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
            "eventId=dff7e9d7-b208-4f08-a2f4-0380119fac88 group=testGroup host=app1 host=app2 tag=eventtag1 host=app3 " +
            "details=\"Really long description with a line break here: \n end of description\" " +
            "tag=eventtag2 host=app4 somerandomannotation=value", out);
    assertEquals(1, out.size());
    assertEquals("dff7e9d7-b208-4f08-a2f4-0380119fac88", out.get(0).getEventId());
    assertEquals("testGroup", out.get(0).getGroup());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(endTs, out.get(0).getEndMillis());
    assertEquals(4, out.get(0).getHosts().size());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals("Really long description with a line break here: \n end of description", out.get(0).getDetails());
    assertEquals(4, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeBasicEventWithQuotedIdentifiers() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "\"type\"=\"deployment-event\" host=\"app1\" severity=INFO \"host\"=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \n end of description\" " +
        "\"tag\"=\"eventtag2\" host=\"app4\" \"somerandomannotation\"=value", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(endTs, out.get(0).getEndMillis());
    assertEquals(4, out.get(0).getHosts().size());
    assertEquals("Really long description with a line break here: \n end of description",
        out.get(0).getDetails());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals(6, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeBasicInstantEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 \"Event name for testing\" " +
        "type=deployment-event host=app1 severity=INFO host=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \n end of description\" " +
        "tag=eventtag2 host=app4 somerandomannotation=value", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(startTs + 1, out.get(0).getEndMillis());
    assertEquals(4, out.get(0).getHosts().size());
    assertEquals("Really long description with a line break here: \n end of description",
        out.get(0).getDetails());
    assertArrayEquals(new String[] {"app1", "app2", "app3", "app4"}, out.get(0).getHosts().toArray());
    assertEquals(6, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeMinimumViableEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 \"Event name for testing\"", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(endTs, out.get(0).getEndMillis());
    assertEquals(0, out.get(0).getHosts().size());
    assertEquals(1, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeMinimumViableEventWithUnquotedName() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 1569423260123 Event_name_for_testing", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(endTs, out.get(0).getEndMillis());
    assertEquals(0, out.get(0).getHosts().size());
    assertEquals(1, out.get(0).getAnnotations().size());
  }

  @Test
  public void testDecodeMinimumViableInstantEvent() {
    List<ReportEvent> out = new ArrayList<>();
    decoder.decode("@Event 1569423200123 \"Event name for testing\"", out);
    assertEquals(1, out.size());
    assertEquals(startTs, out.get(0).getStartMillis());
    assertEquals(startTs + 1, out.get(0).getEndMillis());
    assertEquals(0, out.get(0).getHosts().size());
    assertEquals(1, out.get(0).getAnnotations().size());
  }
}
