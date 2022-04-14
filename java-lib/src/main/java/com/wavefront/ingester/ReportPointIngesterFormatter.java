package com.wavefront.ingester;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.wavefront.common.Clock;
import com.wavefront.data.ParseException;

import wavefront.report.ReportPoint;

/**
 * DEPRECATED: use {@link ReportMetricIngesterFormatter} instead.
 *
 * Builder pattern for creating new ingestion formats. Inspired by the date time formatters in
 * Joda.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@Deprecated
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
                           String customerId, @Nullable List<String> customSourceTags,
                           @Nullable List<String> customLogTimestampTags,
                           @Nullable List<String> customLogMessageTags,
                           List<String> customLogApplicationTags, List<String> customLogServiceTags, @Nullable IngesterContext ingesterContext) {
    ReportPoint point = new ReportPoint();
    point.setTable(customerId);
    // if the point has a timestamp, this would be overriden
    point.setTimestamp(Clock.now());
    final StringParser parser = new StringParser(input);

    for (FormatterElement<ReportPoint> element : elements) {
      element.consume(parser, point);
    }
    if (parser.hasNext()) {
      throw new ParseException("Unexpected extra input: " + parser.next());
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
