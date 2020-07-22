package com.wavefront.ingester;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import wavefront.report.ReportMetric;

public class GraphiteReportMetricDecoder extends ReportMetricDecoder {
  private static final Pattern CUSTOMERID = Pattern.compile("[a-z]+");

  public GraphiteReportMetricDecoder(@Nullable Supplier<String> hostNameSupplier,
                                     List<String> customSourceTags) {
    super(hostNameSupplier, customSourceTags);
  }

  @Override
  public void decode(String msg, List<ReportMetric> out) {
    List<ReportMetric> output = Lists.newArrayList();
    decode(msg, output, "dummy");
    if (!output.isEmpty()) {
      for (ReportMetric rp : output) {
        String metricName = rp.getMetric();
        List<String> metricParts = Lists.newArrayList(Splitter.on(".").split(metricName));
        if (metricParts.size() <= 1) {
          throw new RuntimeException("Metric name does not contain a customer id: " + metricName);
        }
        String customerId = metricParts.get(0);
        if (CUSTOMERID.matcher(customerId).matches()) {
          metricName = Joiner.on(".").join(metricParts.subList(1, metricParts.size()));
        }
        out.add(ReportMetric.newBuilder(rp).setMetric(metricName).setCustomer(customerId).build());
      }
    }
  }

}
