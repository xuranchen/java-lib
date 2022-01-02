package com.wavefront.ingester;

import wavefront.report.ReportMetric;

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
public class ReportMetricDecoder implements ReportableEntityDecoder<String, ReportMetric> {
  private static final AbstractIngesterFormatter<ReportMetric> FORMAT =
      ReportMetricIngesterFormatter.newBuilder().
          text(ReportMetric::setMetric).
          value(ReportMetric::setValue).
          optionalTimestamp(ReportMetric::setTimestamp).
          annotationList(ReportMetric::setAnnotations).
          build();
  private final Supplier<String> hostNameSupplier;
  private List<String> customSourceTags;

  public ReportMetricDecoder(@Nullable Supplier<String> hostNameSupplier,
                             List<String> customSourceTags) {
    this.hostNameSupplier = hostNameSupplier;
    this.customSourceTags = customSourceTags;
  }

  @Override
  public void decode(String msg, List<ReportMetric> out, String customerId, IngesterContext ctx) {
    ReportMetric point = FORMAT.drive(msg, hostNameSupplier, customerId, customSourceTags, null, null,null);
    if (out != null) {
      out.add(point);
    }
  }
}
