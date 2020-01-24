package com.wavefront.ingester;

import com.wavefront.common.Clock;
import com.wavefront.common.MetricConstants;
import wavefront.report.ReportPoint;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Builder pattern for creating new ingestion formats. Inspired by the date time formatters in
 * Joda.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class ReportPointIngesterFormatter extends AbstractIngesterFormatter<ReportPoint> {

  private ReportPointIngesterFormatter(List<FormatterElement<ReportPoint>> elements) {
    super(elements);
  }

  public static class ReportPointIngesterFormatBuilder extends IngesterFormatBuilder<ReportPoint> {
    @Override
    public ReportPointIngesterFormatter build() {
      return new ReportPointIngesterFormatter(elements);
    }
  }

  public static IngesterFormatBuilder<ReportPoint> newBuilder() {
    return new ReportPointIngesterFormatBuilder();
  }

  @Override
  public ReportPoint drive(String input, Supplier<String> defaultHostNameSupplier,
                           String customerId, @Nullable List<String> customSourceTags) {
    ReportPoint point = new ReportPoint();
    point.setTable(customerId);
    // if the point has a timestamp, this would be overriden
    point.setTimestamp(Clock.now());
    final StringParser parser = new StringParser(input);

    try {
      for (FormatterElement<ReportPoint> element : elements) {
        element.consume(parser, point);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }
    if (parser.hasNext()) {
      throw new RuntimeException("Unexpected extra input: " + parser.next());
    }

    // Delta metrics cannot have negative values
    if ((point.getMetric().charAt(0) == MetricConstants.DELTA_PREFIX_CHAR ||
        point.getMetric().charAt(0) == MetricConstants.DELTA_PREFIX_CHAR_2) &&
        point.getValue() instanceof Number) {
      double v = ((Number) point.getValue()).doubleValue();
      if (v <= 0) {
        throw new RuntimeException("Delta metrics cannot be non-positive: " + input);
      }
    }

    String host = null;
    Map<String, String> annotations = point.getAnnotations();
    if (annotations != null) {
      host = annotations.remove("source");
      if (host == null) {
        host = annotations.remove("host");
      } else if (annotations.containsKey("host")) {
        // we have to move this elsewhere since during querying,
        // host= would be interpreted as host and not a point tag
        annotations.put("_host", annotations.remove("host"));
      }
      if (annotations.containsKey("tag")) {
        annotations.put("_tag", annotations.remove("tag"));
      }
      if (host == null && customSourceTags != null) {
        // iterate over the set of custom tags, breaking when one is found
        for (String tag : customSourceTags) {
          host = annotations.get(tag);
          if (host != null) {
            break;
          }
        }
      }
    }
    if (host == null) {
      host = defaultHostNameSupplier.get();
    }
    point.setHost(host);
    return point;
  }
}
