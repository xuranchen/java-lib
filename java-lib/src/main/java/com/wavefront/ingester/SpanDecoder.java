package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;

import wavefront.report.Span;

/**
 * Span decoder that takes in data in the following format:
 *
 * [span name] [annotations] [timestamp] [duration|timestamp]
 *
 * @author vasily@wavefront.com
 */
public class SpanDecoder implements ReportableEntityDecoder<String, Span> {

  private static final AbstractIngesterFormatter<Span> FORMAT = SpanIngesterFormatter.newBuilder().
      text(Span::setName).
      annotationList(Span::setAnnotations, x -> !StringUtils.isNumeric(x)).
      rawTimestamp(Span::setStartMillis).
      rawTimestamp(SpanDecoder::setDuration).
      build();

  private final Supplier<String> hostNameSupplier;

  public SpanDecoder(String hostName) {
    this(() -> hostName);
  }

  public SpanDecoder(Supplier<String> hostNameSupplier) {
    Preconditions.checkNotNull(hostNameSupplier);
    this.hostNameSupplier = hostNameSupplier;
  }

  @Override
  public void decode(String msg, List<Span> out, String customerId, IngesterContext ctx) {
    Span span = FORMAT.drive(msg, hostNameSupplier, customerId, null, ctx);
    if (out != null) {
      out.add(span);
    }
  }

  private static void setDuration(Span span, Long durationTs) {
    Long startTs = span.getStartMillis();
    if (durationTs != null && startTs != null) {
      long duration = (durationTs - startTs >= 0) ? durationTs - startTs : durationTs;
      // convert both timestamps to millis
      if (startTs > 999999999999999999L) {
        // 19 digits == nanoseconds,
        span.setStartMillis(startTs / 1000_000);
        span.setDuration(duration / 1000_000);
      } else if (startTs > 999999999999999L) {
        // 16 digits == microseconds
        span.setStartMillis(startTs / 1000);
        span.setDuration(duration / 1000);
      } else if (startTs > 999999999999L) {
        // 13 digits == milliseconds
        span.setDuration(duration);
      } else {
        // seconds
        span.setStartMillis(startTs * 1000);
        span.setDuration(duration * 1000);
      }
    } else {
      throw new RuntimeException("Both timestamp and duration expected");
    }
  }
}
