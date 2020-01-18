package com.wavefront.ingester;

import com.google.common.annotations.VisibleForTesting;
import wavefront.report.Span;

import java.util.function.Function;

import static com.wavefront.common.SerializerUtils.appendAnnotations;
import static com.wavefront.common.SerializerUtils.appendQuoted;

/**
 * Convert a {@link Span} to its string representation in a canonical format (quoted name and annotations).
 *
 * @author vasily@wavefront.com
 */
public class SpanSerializer implements Function<Span, String> {

  @Override
  public String apply(Span span) {
    return spanToString(span);
  }

  @VisibleForTesting
  static String spanToString(Span span) {
    StringBuilder sb = new StringBuilder();
    appendQuoted(sb, span.getName());
    sb.append(' ');
    if (span.getSource() != null) {
      sb.append("source=");
      appendQuoted(sb, span.getSource()).append(' ');
    }
    if (span.getSpanId() != null) {
      sb.append("spanId=");
      appendQuoted(sb, span.getSpanId()).append(' ');
    }
    if (span.getTraceId() != null) {
      sb.append("traceId=");
      appendQuoted(sb, span.getTraceId());
    }
    appendAnnotations(sb, span.getAnnotations()).
        append(' ').
        append(span.getStartMillis()).
        append(' ').
        append(span.getDuration());
    return sb.toString();
  }
}

