package com.wavefront.ingester;

import org.antlr.v4.runtime.Token;
import wavefront.report.Annotation;
import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Ingestion formatter for events.
 *
 * @author vasily@wavefront.com.
 */
public class EventIngesterFormatter extends AbstractIngesterFormatter<ReportEvent> {

  private EventIngesterFormatter(List<FormatterElement> elements) {
    super(elements);
  }

  /**
   * A builder pattern to create a format for the report point parse.
   */
  public static class EventIngesterFormatBuilder extends IngesterFormatBuilder<ReportEvent> {

    @Override
    public EventIngesterFormatter build() {
      return new EventIngesterFormatter(elements);
    }
  }

  public static IngesterFormatBuilder<ReportEvent> newBuilder() {
    return new EventIngesterFormatBuilder();
  }

  @Override
  public ReportEvent drive(String input, String defaultHostName, String customerId,
                     @Nullable List<String> customSourceTags) {
    Queue<Token> queue = getQueue(input);

    final ReportEvent event = new ReportEvent();
    event.setAnnotations(new HashMap<>());
    EventWrapper wrapper = new EventWrapper(event);
    try {
      for (FormatterElement element : elements) {
        element.consume(queue, wrapper);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }

    for (Annotation annotation : wrapper.getAnnotationList()) {
      switch (annotation.getKey()) {
        case "host":
          if (event.getHosts() == null) {
            event.setHosts(new ArrayList<>());
          }
          event.getHosts().add(annotation.getValue());
          break;
        case "tag":
        case "eventTag":
          if (event.getTags() == null) {
            event.setTags(new ArrayList<>());
          }
          event.getTags().add(annotation.getValue());
          break;
        default:
          Map<String, List<String>> dimensions = event.getDimensions();
          if (dimensions != null && dimensions.containsKey(annotation.getKey())) {
            dimensions.get(annotation.getKey()).add(annotation.getValue());
          } else if (event.getAnnotations().containsKey(annotation.getKey())) {
            // multi-value annotations should be moved to dimensions
            if (dimensions == null) {
              event.setDimensions(new HashMap<>());
            }
            List<String> multivalue = new ArrayList<>();
            multivalue.add(event.getAnnotations().remove(annotation.getKey()));
            multivalue.add(annotation.getValue());
            event.getDimensions().put(annotation.getKey(), multivalue);
          } else {
            event.getAnnotations().put(annotation.getKey(), annotation.getValue());
          }
      }
    }
    // if no end time specified, we assume it's an instant event
    if (event.getEndTime() == null || event.getEndTime() == 0) {
      event.setEndTime(event.getStartTime() + 1);
    }
    return ReportEvent.newBuilder(event).build();
  }
}
