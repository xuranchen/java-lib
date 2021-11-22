package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;
import wavefront.report.Annotation;
import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
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
    event.setAnnotations(new ArrayList<>());
    event.setCustomer(customerId);

    for (FormatterElement<ReportEvent> element : elements) {
      element.consume(parser, event);
    }

    List<Annotation> annotations = event.getAnnotations();
    String defaultGroup = "Default";
    String defaultDetails = null;
    if (annotations != null) {
      Iterator<Annotation> iterator = annotations.iterator();
      while(iterator.hasNext()) {
        final Annotation annotation = iterator.next();
        switch (annotation.getKey()) {
          case "eventId":
            event.setEventId(annotation.getValue());
            iterator.remove();
            break;
          case "host":
            event.getHosts().add(annotation.getValue());
            iterator.remove();
            break;
          case "group":
            event.setGroup(annotation.getValue());
            iterator.remove();
            break;
          case "details":
            event.setDetails(annotation.getValue());
            iterator.remove();
            break;
          case "description":
            defaultDetails = annotation.getValue();
            iterator.remove();
            break;
          case "type":
            defaultGroup = annotation.getValue();
            break;
        }
      }
    }
    if (event.getDetails() == null) {
      // if details field does not exit, use description from the old format
      event.setDetails(defaultDetails);
    } else if (defaultDetails != null){
      // add description back to annotation if details already exist
      event.getAnnotations().add(new Annotation("description", defaultDetails));
    }
    // if no end time specified, we assume it's an instant event
    if (event.getEndMillis() == 0 || event.getEndMillis() <= event.getStartMillis()) {
      event.setEndMillis(event.getStartMillis() + 1);
    }
    if (event.getHosts() == null && defaultHostNameSupplier != null) {
      event.setHosts(ImmutableList.of(defaultHostNameSupplier.get()));
    }
    if (event.getHosts() == null) {
      throw new IllegalArgumentException("hosts can't be null: " + input);
    }
    if (event.getGroup() == null) {
      event.setGroup(defaultGroup);
    }
    if (event.getEventId() == null) {
      event.setEventId(UUID.randomUUID().toString());
    }
    return event;
  }
}
