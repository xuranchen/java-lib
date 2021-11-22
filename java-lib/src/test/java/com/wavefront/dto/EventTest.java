package com.wavefront.dto;

import com.wavefront.ingester.EventDecoder;
import org.junit.Test;
import wavefront.report.ReportEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

/**
 * @author vasily@wavefront.com
 */
public class EventTest {

  @Test
  public void testBasicEvent() {
    List<ReportEvent> out = new ArrayList<>();
    new EventDecoder().decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "type=deployment-event host=app1 severity=INFO host=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \\n end\" " +
        "tag=eventtag2 host=app4 somerandomannotation=value", out);
    out.get(0).setAnnotations(new TreeMap<>(out.get(0).getAnnotations()));
    assertEquals("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
            "\"host\"=\"app1\" \"host\"=\"app2\" \"host\"=\"app3\" \"host\"=\"app4\" " +
            "\"severity\"=\"INFO\" " +
            "\"description\"=\"Really long description with a line break here: \\n end\" " +
            "\"somerandomannotation\"=\"value\" \"type\"=\"deployment-event\" " +
            "\"tag\"=\"eventtag1\" \"tag\"=\"eventtag2\"",
        new Event(out.get(0)).toString());
  }

  @Test
  public void testEventWithDimensions() {
    List<ReportEvent> out = new ArrayList<>();
    new EventDecoder().decode("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
        "type=deployment-event multi=foo host=app1 multi=bar multi2=foo2 severity=INFO " +
        "multi2=bar2 host=app2 multi=baz", out);
    out.get(0).setAnnotations(new TreeMap<>(out.get(0).getAnnotations()));
    out.get(0).setDimensions(new TreeMap<>(out.get(0).getDimensions()));
    assertEquals("@Event 1569423200123 1569423260123 \"Event name for testing\" " +
            "\"host\"=\"app1\" \"host\"=\"app2\" \"severity\"=\"INFO\" " +
            "\"type\"=\"deployment-event\" " +
            "\"multi2\"=\"foo2\" \"multi2\"=\"bar2\" " +
            "\"multi\"=\"foo\" \"multi\"=\"bar\" \"multi\"=\"baz\"",
        new Event(out.get(0)).toString());
  }

  @Test
  public void testInstantEvent() {
    List<ReportEvent> out = new ArrayList<>();
    new EventDecoder().decode("@Event 1569423200123 \"Event name for testing\" " +
        "type=deployment-event host=app1 severity=INFO host=app2 tag=eventtag1 host=app3 " +
        "description=\"Really long description with a line break here: \\n end\" " +
        "tag=eventtag2 host=app4 somerandomannotation=value", out);
    out.get(0).setAnnotations(new TreeMap<>(out.get(0).getAnnotations()));
    assertEquals("@Event 1569423200123 1569423200124 \"Event name for testing\" " +
            "\"host\"=\"app1\" \"host\"=\"app2\" \"host\"=\"app3\" \"host\"=\"app4\" " +
            "\"severity\"=\"INFO\" " +
            "\"description\"=\"Really long description with a line break here: \\n end\" " +
            "\"somerandomannotation\"=\"value\" \"type\"=\"deployment-event\" " +
            "\"tag\"=\"eventtag1\" \"tag\"=\"eventtag2\"",
        new Event(out.get(0)).toString());
  }

  @Test
  public void testMinimumViableEvent() {
    List<ReportEvent> out = new ArrayList<>();
    new EventDecoder().decode("@Event 1569423200123 1569423260123 \"Event name for testing\"", out);
    assertEquals("@Event 1569423200123 1569423260123 \"Event name for testing\"",
        new Event(out.get(0)).toString());
  }

  @Test
  public void testMinimumViableEventWithUnquotedName() {
    List<ReportEvent> out = new ArrayList<>();
    new EventDecoder().decode("@Event 1569423200123 1569423260123 Event_name_for_testing", out);
    assertEquals("@Event 1569423200123 1569423260123 \"Event_name_for_testing\"",
        new Event(out.get(0)).toString());
  }

  @Test
  public void testMinimumViableInstantEvent() {
    List<ReportEvent> out = new ArrayList<>();
    new EventDecoder().decode("@Event 1569423200123 \"Event name for testing\"", out);
    assertEquals("@Event 1569423200123 1569423200124 \"Event name for testing\"",
        new Event(out.get(0)).toString());
  }
}
