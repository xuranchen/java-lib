package com.wavefront.ingester;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Convert a {@link ReportPoint} to its string representation in a canonical format (quoted metric name,
 * tag values and keys (except for "source"). Supports numeric and {@link wavefront.report.Histogram} values.
 *
 * @author vasily@wavefront.com
 */
public class ReportPointSerializer implements Function<ReportPoint, String> {

  @Override
  public String apply(ReportPoint point) {
    return pointToString(point);
  }

  private static String quote = "\"";

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, quote, "\\\"");
  }

  private static void appendTagMap(StringBuilder sb, @Nullable Map<String, String> tags) {
    if (tags == null) {
      return;
    }
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      sb.append(' ').append(quote).append(escapeQuotes(entry.getKey())).append(quote)
          .append("=")
          .append(quote).append(escapeQuotes(entry.getValue())).append(quote);
    }
  }

  private static void appendCompactedCentroids(StringBuilder sb,
                                               List<Double> bins,
                                               List<Integer> counts) {
    int numCentroids = Math.min(bins.size(), counts.size());
    Double accumulatedValue = null;
    int accumulatedCount = 0;
    for (int i = 0; i < numCentroids; ++i) {
      double value = bins.get(i);
      int count = counts.get(i);
      if (accumulatedValue != null && value != accumulatedValue) {
        sb.append('#').append(accumulatedCount).append(' ');
        sb.append(accumulatedValue).append(' ');
        accumulatedValue = value;
        accumulatedCount = count;
      } else {
        if (accumulatedValue == null) {
          accumulatedValue = value;
        }
        accumulatedCount += count;
      }
    }
    if (accumulatedValue != null) {
      sb.append('#').append(accumulatedCount).append(' ');
      sb.append(accumulatedValue).append(' ');
    }
  }

  @VisibleForTesting
  public static String pointToString(ReportPoint point) {
    if (point.getValue() instanceof Double || point.getValue() instanceof Long || point.getValue() instanceof String) {
      StringBuilder sb = new StringBuilder(quote)
          .append(escapeQuotes(point.getMetric())).append(quote).append(" ")
          .append(point.getValue()).append(" ")
          .append(point.getTimestamp() / 1000).append(" ")
          .append("source=").append(quote).append(escapeQuotes(point.getHost())).append(quote);
      appendTagMap(sb, point.getAnnotations());
      return sb.toString();
    } else if (point.getValue() instanceof wavefront.report.Histogram) {
      wavefront.report.Histogram h = (wavefront.report.Histogram) point.getValue();

      StringBuilder sb = new StringBuilder();

      // BinType
      switch (h.getDuration()) {
        case (int) DateUtils.MILLIS_PER_MINUTE:
          sb.append("!M ");
          break;
        case (int) DateUtils.MILLIS_PER_HOUR:
          sb.append("!H ");
          break;
        case (int) DateUtils.MILLIS_PER_DAY:
          sb.append("!D ");
          break;
        default:
          throw new RuntimeException("Unexpected histogram duration " + h.getDuration());
      }
      // Timestamp
      sb.append(point.getTimestamp() / 1000).append(' ');
      // Centroids
      appendCompactedCentroids(sb, h.getBins(), h.getCounts());
      // Metric
      sb.append(quote).append(escapeQuotes(point.getMetric())).append(quote).append(" ");

      // Source
      sb.append("source=").append(quote).append(escapeQuotes(point.getHost())).append(quote);
      appendTagMap(sb, point.getAnnotations());
      return sb.toString();
    }
    throw new RuntimeException("Unsupported value class: " + point.getValue().getClass().getCanonicalName());
  }
}
