package com.wavefront.ingester;

import wavefront.report.ReportEvent;

import java.util.List;

/**
 * Span decoder that takes in data in the following format:
 *
 * { @Event } startTimeMillis [endTimeMillis] eventName [annotations]
 *
 * @author vasily@wavefront.com
 */
public class EventDecoder implements ReportableEntityDecoder<String, ReportEvent> {

  public static final String EVENT = "@Event";

  private static final AbstractIngesterFormatter<ReportEvent> FORMAT =
      EventIngesterFormatter.newBuilder()
          .whiteSpace()
          .appendCaseSensitiveLiterals(new String[]{EVENT}).whiteSpace()
          .appendTimestamp().whiteSpace()
          .appendOptionalEndTimestamp().whiteSpace()
          .appendName().whiteSpace()
          .appendAnnotationsConsumer().whiteSpace()
          .build();

  public EventDecoder() {
  }

  @Override
  public void decode(String msg, List<ReportEvent> out, String customerId) {
    ReportEvent event = FORMAT.drive(msg, "default", "default");
    if (out != null) {
      out.add(event);
    }
  }
}
