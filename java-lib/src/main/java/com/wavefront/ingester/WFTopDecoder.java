package com.wavefront.ingester;

import wavefront.report.ReportPoint;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Decoder for WFTop data format (Spy API stream):
 *
 * [metric] source=[source] [timestamp] [value] [annotations]
 *
 * @author vasily@wavefront.com
 */
@Deprecated
public class WFTopDecoder implements Decoder<String> {
  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      ReportPointIngesterFormatter.newBuilder().
          text(ReportPoint::setMetric).
          annotationMap(ReportPoint::setAnnotations, 1).
          timestamp(ReportPoint::setTimestamp).
          value(ReportPoint::setValue).
          annotationMap(ReportPoint::getAnnotations, ReportPoint::setAnnotations).
          build();
  private final Supplier<String> hostNameSupplier = () -> "default";

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, hostNameSupplier, customerId, Collections.emptyList(), null, null, null, null, null, null, null);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId, IngesterContext ingesterContext) {
    ReportPoint point = FORMAT.drive(msg, hostNameSupplier, customerId, Collections.emptyList(), null, null, null, null, null, null, ingesterContext);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    throw new UnsupportedOperationException("Extracting customer ID is not supported!");
  }
}
