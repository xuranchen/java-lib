package com.wavefront.ingester;

import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Ingestion formatter for events.
 *
 * @author vasily@wavefront.com.
 */
public class EventIngesterFormatter extends AbstractIngesterFormatter<ReportEvent> {

  private EventIngesterFormatter(List<FormatterElement<ReportEvent>> elements) {
    super(elements);
  }

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
  public ReportEvent drive(String input, Supplier<String> defaultHostNameSupplier,
                           String customerId, @Nullable List<String> customSourceTags,
                           @Nullable IngesterContext ingesterContext) {
    final ReportEvent event = new ReportEvent();
    StringParser parser = new StringParser(input);
    event.setHosts(new ArrayList<>());
    event.setAnnotations(new HashMap<>());

    try {
      for (FormatterElement<ReportEvent> element : elements) {
        if (ingesterContext != null) {
          element.consume(parser, event, ingesterContext);
        } else {
          element.consume(parser, event);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }

    Iterator<Map.Entry<String, List<String>>> iter = event.getDimensions().entrySet().iterator();
    while (iter.hasNext()) {
      final Map.Entry<String, List<String>> entry = iter.next();
      switch (entry.getKey()) {
        case "host":
          event.setHosts(entry.getValue());
          iter.remove();
          break;
        case "tag":
        case "eventTag":
          event.setTags(entry.getValue());
          iter.remove();
          break;
        default:
          // single-value dimensions should be moved to annotations
          if (entry.getValue().size() == 1) {
            event.getAnnotations().put(entry.getKey(), entry.getValue().get(0));
            iter.remove();
          }
      }
    }
    if (event.getDimensions().isEmpty()) {
      event.setDimensions(null);
    }
    // if no end time specified, we assume it's an instant event
    if (event.getEndTime() == null || event.getEndTime() == 0) {
      event.setEndTime(event.getStartTime() + 1);
    }
    return event;
  }
}
