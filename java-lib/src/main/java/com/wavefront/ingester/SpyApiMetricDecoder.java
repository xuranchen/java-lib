package com.wavefront.ingester;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import wavefront.report.ReportMetric;

/**
 * Decoder for WFTop data format (Spy API stream):
 *
 * [metric] source=[source] [timestamp] [value] [annotations]
 *
 * @author vasily@wavefront.com
 */
public class SpyApiMetricDecoder implements ReportableEntityDecoder<String, ReportMetric> {
  private static final AbstractIngesterFormatter<ReportMetric> FORMAT =
      ReportMetricIngesterFormatter.newBuilder().
          text(ReportMetric::setMetric).
          annotationList(ReportMetric::setAnnotations, 1).
          timestamp(ReportMetric::setTimestamp).
          value(ReportMetric::setValue).
          annotationList(ReportMetric::getAnnotations, ReportMetric::setAnnotations).
          build();
  private final Supplier<String> hostNameSupplier = () -> "default";

  @Override
  public void decode(String msg, List<ReportMetric> out, String customerId, IngesterContext ctx) {
    ReportMetric point = FORMAT.drive(msg, hostNameSupplier, customerId, Collections.emptyList(),
        ctx);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decode(String msg, List<ReportMetric> out) {
    throw new UnsupportedOperationException("Extracting customer ID is not supported!");
  }
}
