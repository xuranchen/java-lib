package com.wavefront.ingester;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang.time.DateUtils;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.data.DataValidationException;

import wavefront.report.Histogram;
import wavefront.report.ReportHistogram;

import static com.wavefront.common.SerializerUtils.appendAnnotations;
import static com.wavefront.common.SerializerUtils.appendQuoted;

/**
 * Convert a {@link ReportHistogram} to its string representation in a canonical format
 * (quoted metric name, tag values and keys (except for "source"). S
 *
 * @author vasily@wavefront.com
 */
public class ReportHistogramSerializer implements Function<ReportHistogram, String> {

  @Override
  public String apply(ReportHistogram point) {
    return histogramToString(point);
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
  public static String histogramToString(ReportHistogram point) {
    Histogram h = point.getValue();
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
        throw new DataValidationException("Unexpected histogram duration " + h.getDuration());
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
    appendAnnotations(sb, point.getAnnotations());
    return sb.toString();
  }
}
