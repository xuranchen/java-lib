package com.wavefront.ingester;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import wavefront.report.ReportPoint;

import javax.annotation.Nullable;

/**
 * DEPRECATED: use {@link ReportMetricDecoder} instead.
 *
 * Graphite decoder that takes in a point of the type:
 *
 * [metric] [value] [timestamp] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@Deprecated
public class GraphiteDecoder implements Decoder<String> {

  private static final Pattern CUSTOMERID = Pattern.compile("[a-z]+");

  private final ReportableEntityDecoder<String, ReportPoint> pointDecoder;

  public GraphiteDecoder(List<String> customSourceTags) {
    this("unknown", customSourceTags);
  }

  public GraphiteDecoder(String hostName, List<String> customSourceTags) {
    this(() -> hostName, customSourceTags);
  }

  public GraphiteDecoder(@Nullable Supplier<String> hostNameSupplier,
                         List<String> customSourceTags) {
    this.pointDecoder = new ReportPointDecoder(hostNameSupplier, customSourceTags);
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    pointDecoder.decode(msg, out, customerId);
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId,
                                 IngesterContext ctx) {
    pointDecoder.decode(msg, out, customerId, null);
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    List<ReportPoint> output = Lists.newArrayList();
    decodeReportPoints(msg, output, "dummy");
    if (!output.isEmpty()) {
      for (ReportPoint rp : output) {
        String metricName = rp.getMetric();
        List<String> metricParts = Lists.newArrayList(Splitter.on(".").split(metricName));
        if (metricParts.size() <= 1) {
          throw new RuntimeException("Metric name does not contain a customer id: " + metricName);
        }
        String customerId = metricParts.get(0);
        if (CUSTOMERID.matcher(customerId).matches()) {
          metricName = Joiner.on(".").join(metricParts.subList(1, metricParts.size()));
        }
        out.add(ReportPoint.newBuilder(rp).setMetric(metricName).setTable(customerId).build());
      }
    }
  }
}
