package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;
import wavefront.report.ReportEvent;

import java.util.List;

import static com.wavefront.ingester.AbstractIngesterFormatter.EVENT_LITERAL;

/**
 * Event decoder that takes in data in the following format:
 *
 * { @Event } startTimeMillis [endTimeMillis] eventName [annotations]
 *
 * @author vasily@wavefront.com
 */
public class EventDecoder implements ReportableEntityDecoder<String, ReportEvent> {

  private static final AbstractIngesterFormatter<ReportEvent> FORMAT =
      EventIngesterFormatter.newBuilder().
          caseSensitiveLiterals(ImmutableList.of(EVENT_LITERAL)).
          timestamp(ReportEvent::setStartTime).
          optionalTimestamp(ReportEvent::setEndTime).
          text(ReportEvent::setName).
          annotationMultimap(ReportEvent::setDimensions).
          build();

  @Override
  public void decode(String msg, List<ReportEvent> out, String customerId) {
    ReportEvent event = FORMAT.drive(msg, null, "default");
    if (out != null) {
      out.add(event);
    }
  }
}
