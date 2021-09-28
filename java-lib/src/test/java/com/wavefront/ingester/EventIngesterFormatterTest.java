package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.ReportEvent;

import java.util.function.Supplier;

import static com.wavefront.ingester.AbstractIngesterFormatter.EVENT_LITERAL;
import static org.junit.Assert.assertEquals;

public class EventIngesterFormatterTest {
    final AbstractIngesterFormatter<ReportEvent> subject =
            EventIngesterFormatter.newBuilder().
                    caseSensitiveLiterals(ImmutableList.of(EVENT_LITERAL)).
                    timestamp(ReportEvent::setStartMillis).
                    optionalTimestamp(ReportEvent::setEndMillis).
                    annotationText(ReportEvent::getAnnotations, ReportEvent::setAnnotations, "eventName").
                    annotationList(ReportEvent::getAnnotations, ReportEvent::setAnnotations).
                    build();
    final Supplier<String> defaultHostNameSupplier = () -> "";
    final String customerId = "";

    @Test
    public void setsEventGroupFromAnnotation() {
        String input = "@Event 1569423200123 1569423260123 eventName group=expectedGroup";

        ReportEvent actual = subject.drive(input, defaultHostNameSupplier, customerId, null, null);

        assertEquals("expectedGroup", actual.getGroup());
    }

    @Test
    public void setsEventGroupFromTypeAnnotation() {
        String input = "@Event 1569423200123 1569423260123 eventName type=\"expectedType\"";

        ReportEvent actual = subject.drive(input, defaultHostNameSupplier, customerId, null, null);

        assertEquals("expectedType", actual.getGroup());
    }

    @Test
    public void groupAnnotationOverridesTypeAnnotation() {
        String input = "@Event 1569423200123 1569423260123 eventName " +
                "group=expectedGroup type=someOtherValue";

        ReportEvent actual = subject.drive(input, defaultHostNameSupplier, customerId, null, null);

        assertEquals("expectedGroup", actual.getGroup());
    }

    @Test
    public void typeTagIsPreserved() {
        String input = "@Event 1569423200123 1569423260123 eventName " +
                "foo=bar type=\"expectedType\" anotherAnnotation=another";

        ReportEvent actual = subject.drive(input, defaultHostNameSupplier, customerId, null, null);

        ImmutableList<Annotation> expected = ImmutableList.of(
                new Annotation("eventName", "eventName"),
                new Annotation("foo", "bar"),
                new Annotation("type", "expectedType"),
                new Annotation("anotherAnnotation", "another"));

        assertEquals(expected, actual.getAnnotations());
    }
}