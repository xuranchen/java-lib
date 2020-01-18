package com.wavefront.ingester;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.time.DateUtils;
import wavefront.report.ReportPoint;

import java.util.List;
import java.util.function.Function;

import static com.wavefront.common.SerializerUtils.appendQuoted;
import static com.wavefront.common.SerializerUtils.appendTagMap;

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
    if (point.getValue() instanceof Number || point.getValue() instanceof String) {
      StringBuilder sb = new StringBuilder(80);
      appendQuoted(sb, point.getMetric()).
          append(" ").append(point.getValue()).
          append(" ").append(point.getTimestamp() / 1000).
          append(" ").append("source=");
      appendQuoted(sb, point.getHost());
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
      appendQuoted(sb, point.getMetric());
      // Source
      sb.append(" ").append("source=");
      appendQuoted(sb, point.getHost());
      appendTagMap(sb, point.getAnnotations());
      return sb.toString();
    }
    throw new RuntimeException("Unsupported value class: " +
        point.getValue().getClass().getCanonicalName());
  }
}
