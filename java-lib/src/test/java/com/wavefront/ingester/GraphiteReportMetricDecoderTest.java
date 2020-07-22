package com.wavefront.ingester;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import wavefront.report.ReportMetric;

import static com.wavefront.data.AnnotationUtils.getValue;
import static java.util.Collections.emptyList;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class GraphiteReportMetricDecoderTest {
  @Test
  public void testDoubleFormat() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93.123e3 host=vehicle_2554", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93123.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level -93.123e3 host=vehicle_2554", out);
    point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-93123.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level -93.123e3", out);
    point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-93123.0, point.getValue());
    assertEquals("localhost", point.getHost());
    assertNotNull(point.getAnnotations());
    assertTrue(point.getAnnotations().isEmpty());

    out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93.123e-3 host=vehicle_2554", out);
    point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(0.093123, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level -93.123e-3 host=vehicle_2554", out);
    point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-0.093123, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    List<ReportMetric> points = Lists.newArrayList();
    decoder.decode("test.devnag.10 100 host=ip1", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());

    points = Lists.newArrayList();
    decoder.decode("∆test.devnag.10 100 host=ip1", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("∆test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());

    points.clear();
    decoder.decode("test.devnag.10 100 host=ip1 a=500", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("500", getValue(point.getAnnotations(), "a"));
    points.clear();
    decoder.decode("test.devnag.10 100 host=ip1 b=500", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("500", getValue(point.getAnnotations(), "b"));
    points.clear();
    decoder.decode("test.devnag.10 100 host=ip1 A=500", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("500", getValue(point.getAnnotations(), "A"));
  }

  @Test
  public void testTagVWithDigitAtBeginning() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93 host=vehicle_2554 version=1_0", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
    assertEquals("1_0", getValue(point.getAnnotations(), "version"));
  }

  @Test
  public void testFormat() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93 host=vehicle_2554", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testIpV4Host() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93 host=10.0.0.1", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("10.0.0.1", point.getHost());
  }

  @Test
  public void testIpV6Host() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93 host=2001:db8:3333:4444:5555:6666:7777:8888", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("2001:db8:3333:4444:5555:6666:7777:8888", point.getHost());
  }

  @Test
  public void testFormatWithTimestamp() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93 1234567890.246 host=vehicle_2554", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals(1234567890246L, point.getTimestamp());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testFormatWithNoTags() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
  }

  @Test
  public void testFormatWithNoTagsAndTimestamp() throws Exception {
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("tsdb.vehicle.charge.battery_level 93 1234567890.246", out);
    ReportMetric point = out.get(0);
    assertEquals("tsdb", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp());
  }

  @Ignore
  @Test
  public void testBenchmark() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    customSourceTags.add("hostname");
    customSourceTags.add("highcardinalitytag1");
    customSourceTags.add("highcardinalitytag2");
    customSourceTags.add("highcardinalitytag3");
    customSourceTags.add("highcardinalitytag4");
    customSourceTags.add("highcardinalitytag5");
    ReportMetricDecoder decoder = new GraphiteReportMetricDecoder(() -> "localhost", emptyList());
    int ITERATIONS = 1000000;
    for (int i = 0; i < ITERATIONS / 1000; i++) {
      List<ReportMetric> out = new ArrayList<>();
      decoder.decode("tsdb.vehicle.charge.battery_level 93 123456 highcardinalitytag5=vehicle_2554", out);
    }
    long start = System.currentTimeMillis();
    for (int i = 0; i < ITERATIONS; i++) {
      List<ReportMetric> out = new ArrayList<>();
      decoder.decode("tsdb.vehicle.charge.battery_level 93 123456 highcardinalitytag5=vehicle_2554", out);
    }
    double end = System.currentTimeMillis();
    System.out.println(ITERATIONS / ((end - start) / 1000) + " DPS");
  }


}
