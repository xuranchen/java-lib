package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;

import com.google.common.collect.ImmutableList;
import wavefront.report.ReportPoint;

/**
 * OpenTSDB decoder that takes in a point of the type:
 *
 * PUT [metric] [timestamp] [value] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@Deprecated
public class OpenTSDBDecoder implements Decoder<String> {

  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      ReportPointIngesterFormatter.newBuilder().
          caseInsensitiveLiterals(ImmutableList.of("put")).
          text(ReportPoint::setMetric).
          timestamp(ReportPoint::setTimestamp).
          value(ReportPoint::setValue).
          annotationMap(ReportPoint::setAnnotations).
          build();
  private final String hostName;
  private List<String> customSourceTags;

  public OpenTSDBDecoder(List<String> customSourceTags) {
    this("unknown", customSourceTags);
  }

  public OpenTSDBDecoder(String hostName, List<String> customSourceTags) {
    Preconditions.checkNotNull(hostName);
    Preconditions.checkNotNull(customSourceTags);
    this.hostName = hostName;
    this.customSourceTags = customSourceTags;
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, () -> hostName, customerId, customSourceTags, null);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId, IngesterContext ingesterContext) {
    ReportPoint point = FORMAT.drive(msg, () -> hostName, customerId, customSourceTags, ingesterContext);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    decodeReportPoints(msg, out, "dummy");
  }
}