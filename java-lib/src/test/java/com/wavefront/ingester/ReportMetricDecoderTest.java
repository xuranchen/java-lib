package com.wavefront.ingester;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import wavefront.report.ReportMetric;

import static com.wavefront.data.AnnotationUtils.getValue;
import static java.util.Collections.emptyList;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO: Edit this <p/> User: sam Date: 7/13/13 Time: 11:34 AM
 */
public class ReportMetricDecoderTest {

  @Test
  public void testDecodeWithNoCustomer() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vehicle.charge.battery_level 93 host=vehicle_2554", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testDecodeWithNoCustomerWithTimestamp() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vehicle.charge.battery_level 93 1234567890.246 host=vehicle_2554", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals(1234567890246L, point.getTimestamp());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testDecodeWithNoCustomerWithNoTags() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vehicle.charge.battery_level 93", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
  }

  @Test
  public void testDecodeWithNoCustomerWithNoTagsAndTimestamp() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vehicle.charge.battery_level 93 1234567890.246", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp());
  }

  @Test
  public void testDecodeWithMillisTimestamp() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vehicle.charge.battery_level 93 1234567892468", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567892468L, point.getTimestamp());
  }

  @Test
  public void testMetricWithNumberStarting() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("1vehicle.charge.battery_level 93 1234567890.246", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("1vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp());
  }

  @Test
  public void testQuotedMetric() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("\"1vehicle.charge.$()+battery_level\" 93 1234567890.246 host=12345 " +
        "blah=\"test hello\" \"hello world\"=test", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("1vehicle.charge.$()+battery_level", point.getMetric());
    assertEquals("12345", point.getHost());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp());
    assertEquals("test hello", getValue(point.getAnnotations(), "blah"));
    assertEquals("test", getValue(point.getAnnotations(), "hello world"));
  }

  @Test
  public void testMetricWithAnnotationQuoted() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("1vehicle.charge.battery_level 93 1234567890.246 host=12345 blah=\"test hello\" " +
        "\"hello world\"=test", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("1vehicle.charge.battery_level", point.getMetric());
    assertEquals("12345", point.getHost());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp());
    assertEquals("test hello", getValue(point.getAnnotations(), "blah"));
    assertEquals("test", getValue(point.getAnnotations(), "hello world"));
  }

  @Test
  public void testQuotes() {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("\"1vehicle.charge.'battery_level\" 93 1234567890.246 " +
        "host=12345 blah=\"test'\\\"hello\\\"\" \"hello world\"='\"test\\''", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("1vehicle.charge.'battery_level", point.getMetric());
    assertEquals("12345", point.getHost());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp());
    assertEquals("test'\"hello\"", getValue(point.getAnnotations(), "blah"));
    assertEquals("\"test'", getValue(point.getAnnotations(), "hello world"));
  }

  @Test
  public void testSimple() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("test 1 host=test", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testSource() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("test 1 source=test", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testSourcePriority() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, customSourceTags);
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("test 1 source=test host=bar fqdn=foo", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals("bar", getValue(point.getAnnotations(), "_host"));
    assertEquals("foo", getValue(point.getAnnotations(), "fqdn"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testFQDNasSource() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    customSourceTags.add("hostname");
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, customSourceTags);
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("test 1 hostname=machine fqdn=machine.company.com", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("test", point.getMetric());
    assertEquals("machine.company.com", point.getHost());
    assertEquals("machine.company.com", getValue(point.getAnnotations(), "fqdn"));
    assertEquals("machine", getValue(point.getAnnotations(), "hostname"));
    assertEquals(1.0, point.getValue());
  }


  @Test
  public void testUserPrefOverridesDefault() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    ReportMetricDecoder decoder = new ReportMetricDecoder(() -> "localhost", customSourceTags);
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("test 1 fqdn=machine.company.com", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("test", point.getMetric());
    assertEquals("machine.company.com", point.getHost());
    assertEquals("machine.company.com", getValue(point.getAnnotations(), "fqdn"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testTagRewrite() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("test 1 source=test tag=bar", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals("bar", getValue(point.getAnnotations(), "_tag"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testMONIT2576() {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.corp.com guest=47173170-2069-4bcc-9bd4-041055b554ec " +
        "instance=ide0_0", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.corp.com", point.getHost());
    assertEquals("47173170-2069-4bcc-9bd4-041055b554ec", getValue(point.getAnnotations(), "guest"));
    assertEquals("ide0_0", getValue(point.getAnnotations(), "instance"));
    assertEquals(4.0, point.getValue());

    out = new ArrayList<>();
    try {
      decoder.decode("test.metric 1 host=test test=\"", out, "customer");
      fail("should throw");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testNumberLookingTagValue() {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.corp.com version=\"1.0.0-030051.d0e485f\"", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.corp.com", point.getHost());
    assertEquals("1.0.0-030051.d0e485f", getValue(point.getAnnotations(), "version"));
    assertEquals(4.0, point.getValue());
  }

  @Test
  public void testNumberLookingTagValue2() {
    ReportMetricDecoder decoder = new ReportMetricDecoder(null, emptyList());
    List<ReportMetric> out = Lists.newArrayList();
    decoder.decode("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.corp.com version=\"1.0.0\\\"-030051.d0e485f\"", out, "customer");
    ReportMetric point = out.get(0);
    assertEquals("customer", point.getCustomer());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.corp.com", point.getHost());
    assertEquals("1.0.0\"-030051.d0e485f", getValue(point.getAnnotations(), "version"));
    assertEquals(4.0, point.getValue());
  }

  @Test
  public void testPositiveDeltas() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(() -> "localhost", emptyList());
    List<ReportMetric> out = new ArrayList<>();
    try {
      decoder.decode("∆request.count 1 source=test.wavefront.com", out);
      decoder.decode("Δrequest.count 1 source=test.wavefront.com", out);
    } catch (RuntimeException e) {
      fail("should not throw");
    }
  }
}
