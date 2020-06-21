package com.wavefront.ingester;

import wavefront.report.ReportPoint;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

/**
 * Graphite decoder that takes in a point of the type:
 *
 * [metric] [value] [timestamp] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class ReportPointDecoder implements ReportableEntityDecoder<String, ReportPoint> {
  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      ReportPointIngesterFormatter.newBuilder().
          text(ReportPoint::setMetric).
          value(ReportPoint::setValue).
          optionalTimestamp(ReportPoint::setTimestamp).
          annotationMap(ReportPoint::setAnnotations).
          build();
  private final Supplier<String> hostNameSupplier;
  private List<String> customSourceTags;

  public ReportPointDecoder(@Nullable Supplier<String> hostNameSupplier,
                            List<String> customSourceTags) {
    this.hostNameSupplier = hostNameSupplier;
    this.customSourceTags = customSourceTags;
  }

  @Override
  public void decode(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, hostNameSupplier, customerId, customSourceTags, null);
    if (out != null) {
      out.add(point);
    }
  }
}
