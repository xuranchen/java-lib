package com.wavefront.ingester;

import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.time.DateUtils;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;

/**
 * Decoder that takes in histograms of the type:
 *
 * [BinType] [Timestamp] [Centroids] [Metric] [Annotations]
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class HistogramDecoder implements Decoder<String> {

  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      ReportPointIngesterFormatter.newBuilder().
          caseSensitiveLiterals(ImmutableList.of("!M", "!H", "!D"), HistogramDecoder::setBinType).
          optionalTimestamp(ReportPoint::setTimestamp).
          centroids().
          text(ReportPoint::setMetric).
          annotationMap(ReportPoint::setAnnotations).
          build();

  private final Supplier<String> defaultHostNameSupplier;

  public HistogramDecoder() {
    this("unknown");
  }

  public HistogramDecoder(String defaultHostName) {
    this(() -> defaultHostName);
  }

  public HistogramDecoder(Supplier<String> defaultHostNameSupplier) {
    this.defaultHostNameSupplier = defaultHostNameSupplier;
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, defaultHostNameSupplier, customerId);
    if (point != null) {
      // adjust timestamp according to histogram bin first
      long duration = ((Histogram) point.getValue()).getDuration();
      point.setTimestamp((point.getTimestamp() / duration) * duration);
      out.add(ReportPoint.newBuilder(point).build());
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId, IngesterContext ingesterContext) {
    ReportPoint point = FORMAT.drive(msg, defaultHostNameSupplier, customerId, ingesterContext);
    if (point != null) {
      // adjust timestamp according to histogram bin first
      long duration = ((Histogram) point.getValue()).getDuration();
      point.setTimestamp((point.getTimestamp() / duration) * duration);
      out.add(ReportPoint.newBuilder(point).build());
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    throw new UnsupportedOperationException("Customer ID extraction is not supported");
  }

  private static void setBinType(ReportPoint target, String binType) {
    int durationMillis;
    switch (binType) {
      case "!M":
        durationMillis = (int) DateUtils.MILLIS_PER_MINUTE;
        break;
      case "!H":
        durationMillis = (int) DateUtils.MILLIS_PER_HOUR;
        break;
      case "!D":
        durationMillis = (int) DateUtils.MILLIS_PER_DAY;
        break;
      default:
        throw new RuntimeException("Unknown BinType " + binType);
    }
    Histogram histogram = new Histogram();
    histogram.setDuration(durationMillis);
    histogram.setType(HistogramType.TDIGEST);
    target.setValue(histogram);
  }
}
