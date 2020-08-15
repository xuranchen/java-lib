package com.wavefront.ingester;

import com.wavefront.common.Clock;
import com.wavefront.data.TooManyCentroidException;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.ReportHistogram;

import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.ingester.IngesterContext.DEFAULT_CENTROIDS_COUNT_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class ReportHistogramDecoderTest {

  @Test
  public void testBasicMessage() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 1471988653 #3 123.237 TestMetric source=Test key=value", out, "customer",
        new IngesterContext.Builder().withTargetHistogramAccuracy(32).throwIfTooManyHistogramCentroids(10).build());

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);
    assertThat(p.getMetric()).isEqualTo("TestMetric");
    // Should be converted to Millis and pinned to the beginning of the corresponding minute
    assertThat(p.getTimestamp()).isEqualTo(1471988640000L);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    assertThat(p.getHost()).isEqualTo("Test");
    assertThat(p.getCustomer()).isEqualTo("customer");
    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).contains(new Annotation("key", "value"));

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_MINUTE);
    assertThat(h.getBins()).isNotNull();
    assertThat(h.getBins()).isNotEmpty();
    assertThat(h.getBins()).containsExactly(123.237D);
    assertThat(h.getCounts()).isNotNull();
    assertThat(h.getCounts()).isNotEmpty();
    assertThat(h.getCounts()).containsExactly(3);
  }


  @Test
  public void testHourBin() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!H 1471988653 #3 123.237 TestMetric source=Test key=value", out, "customer");

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);
    // Should be converted to Millis and pinned to the beginning of the corresponding hour
    assertThat(p.getTimestamp()).isEqualTo(1471986000000L);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_HOUR);
  }

  @Test
  public void testDayBin() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!D 1471988653 #3 123.237 TestMetric source=Test key=value", out, "customer");

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);
    // Should be converted to Millis and pinned to the beginning of the corresponding day
    assertThat(p.getTimestamp()).isEqualTo(1471910400000L);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_DAY);
  }

  @Test
  public void testTagKey() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 1471988653 #3 123.237 TestMetric source=Test tag=value", out, "customer");

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);

    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).contains(new Annotation("_tag", "value"));
  }

  @Test
  public void testMultipleBuckets() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 1471988653 #1 3.1416 #1 2.7183 TestMetric", out, "customer");

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_MINUTE);
    assertThat(h.getBins()).isNotNull();
    assertThat(h.getBins()).isNotEmpty();
    assertThat(h.getBins()).containsExactly(3.1416D, 2.7183D);
    assertThat(h.getCounts()).isNotNull();
    assertThat(h.getCounts()).isNotEmpty();
    assertThat(h.getCounts()).containsExactly(1, 1);
  }

  @Test
  public void testNegativeMean() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 1471988653 #1 -3.1416 TestMetric", out, "customer");

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_MINUTE);
    assertThat(h.getBins()).isNotNull();
    assertThat(h.getBins()).isNotEmpty();
    assertThat(h.getBins()).containsExactly(-3.1416D);
    assertThat(h.getCounts()).isNotNull();
    assertThat(h.getCounts()).isNotEmpty();
    assertThat(h.getCounts()).containsExactly(1);
  }

  @Test(expected = RuntimeException.class)
  public void testMissingBin() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("1471988653 #3 123.237 TestMetric source=Test tag=value", out, "customer");
  }

  @Test
  public void testMissingTimestamp() {
    //Note - missingTimestamp to port 40,000 is no longer invalid - see MONIT-6430 for more details
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #3 123.237 TestMetric source=Test tag=value", out, "customer");

    assertThat(out).isNotEmpty();
    long expectedTimestamp = Clock.now();

    ReportHistogram p = out.get(0);
    assertThat(p.getMetric()).isEqualTo("TestMetric");

    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    // Should be converted to Millis and pinned to the beginning of the corresponding minute
    long duration = ((Histogram) p.getValue()).getDuration();
    expectedTimestamp = (expectedTimestamp / duration) * duration;
    assertThat(p.getTimestamp()).isEqualTo(expectedTimestamp);

    assertThat(p.getHost()).isEqualTo("Test");
    assertThat(p.getCustomer()).isEqualTo("customer");
    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).contains(new Annotation("_tag", "value"));

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_MINUTE);
    assertThat(h.getBins()).isNotNull();
    assertThat(h.getBins()).isNotEmpty();
    assertThat(h.getBins()).containsExactly(123.237D);
    assertThat(h.getCounts()).isNotNull();
    assertThat(h.getCounts()).isNotEmpty();
    assertThat(h.getCounts()).containsExactly(3);
  }

  @Test(expected = RuntimeException.class)
  public void testMissingCentroids() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 1471988653 TestMetric source=Test tag=value", out, "customer");
  }

  @Test
  public void testMissingMean() {
    //Note - missingTimestamp to port 40,000 is no longer invalid - see MONIT-6430 for more details
    // as a side-effect of that, this test no longer fails!!!
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #3 1471988653 TestMetric source=Test tag=value", out, "customer");

    assertThat(out).isNotEmpty();
    long expectedTimestamp = Clock.now();

    ReportHistogram p = out.get(0);
    assertThat(p.getMetric()).isEqualTo("TestMetric");

    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    // Should be converted to Millis and pinned to the beginning of the corresponding minute
    long duration = ((Histogram) p.getValue()).getDuration();
    expectedTimestamp = (expectedTimestamp / duration) * duration;
    assertThat(p.getTimestamp()).isEqualTo(expectedTimestamp);

    assertThat(p.getHost()).isEqualTo("Test");
    assertThat(p.getCustomer()).isEqualTo("customer");
    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).contains(new Annotation("_tag", "value"));

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_MINUTE);
    assertThat(h.getBins()).isNotNull();
    assertThat(h.getBins()).isNotEmpty();
    assertThat(h.getBins()).containsExactly(1471988653.0);
    assertThat(h.getCounts()).isNotNull();
    assertThat(h.getCounts()).isNotEmpty();
    assertThat(h.getCounts()).containsExactly(3);
  }

  @Test(expected = RuntimeException.class)
  public void testMissingCount() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 3.412 1471988653 TestMetric source=Test tag=value", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testZeroCount() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #0 3.412 1471988653 TestMetric source=Test tag=value", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testMissingMetric() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("1471988653 #3 123.237 source=Test tag=value", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testMissingCentroid() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #1 TestMetric source=Test", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testMissingCentroid2() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #1 12345 #2 TestMetric source=Test", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testTooManyCentroids() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #1 12345 #2 123453 1234534 12334 TestMetric source=Test", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testNoCentroids() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M 12334 TestMetric source=Test", out, "customer");
  }

  @Test
  public void testNoSourceAnnotationsIsNotNull() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    decoder.decode("!M #1 12334 TestMetric", out, "customer");

    ReportHistogram p = out.get(0);
    assertNotNull(p.getAnnotations());
  }

  @Test
  public void testDefaultThrowTooManyCentroidsException() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    IngesterContext ingesterContext = new IngesterContext.Builder().
        throwIfTooManyHistogramCentroids(DEFAULT_CENTROIDS_COUNT_LIMIT).build();

    // Assert we are limit to 100.
    assertEquals(DEFAULT_CENTROIDS_COUNT_LIMIT, 100);

    // Test 100 centroids can pass.
    StringBuilder histogramSB = new StringBuilder();
    histogramSB.append("!M 1471988653");
    for (int i = 1; i <= DEFAULT_CENTROIDS_COUNT_LIMIT; i++) {
      histogramSB.append(" #").append(i).append(" ").append((double) i);
    }
    histogramSB.append(" TestMetric source=Test key=value");
    decoder.decode(histogramSB.toString(), out, "customer", ingesterContext);
    assertThat(out).isNotEmpty();
    out.clear();

    // Test 101 centroids will throw TooManyCentroidException.
    histogramSB = new StringBuilder();
    histogramSB.append("!M 1471988653");
    for (int i = 1; i <= DEFAULT_CENTROIDS_COUNT_LIMIT + 1; i++) {
      histogramSB.append(" #").append(i).append(" ").append((double) i);
    }
    histogramSB.append(" TestMetric source=Test key=value");

    try {
      decoder.decode(histogramSB.toString(), out, "customer", ingesterContext);
      fail();
    } catch (TooManyCentroidException e) {
      // OK
    }
  }

  @Test(expected = TooManyCentroidException.class)
  public void testCustomThrowTooManyCentroidsException() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    int centroidsLimit = 50;

    // Test we using custom IngesterContext with centroids limit to 50.
    IngesterContext ingesterContext = new IngesterContext.Builder().withTargetHistogramAccuracy(32).
            throwIfTooManyHistogramCentroids(centroidsLimit).build();

    StringBuilder histogramSB = new StringBuilder();
    histogramSB.append("!M 1471988653");
    for (int i = 1; i <= centroidsLimit + 1; i++) {
      histogramSB.append(" #").append(i).append(" ").append((double) i);
    }
    histogramSB.append(" TestMetric source=Test key=value");

    decoder.decode(histogramSB.toString(), out, "customer", ingesterContext);
  }

  @Test
  public void testHistogramOptimization() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();

    IngesterContext ingesterContext = new IngesterContext.Builder().withTargetHistogramAccuracy(8).
        withOptimizeHistograms(true).build();

    int centroidsLimit = 50;
    StringBuilder histogramSB = new StringBuilder();
    histogramSB.append("!M 1471988653");
    for (int i = 1; i < centroidsLimit + 1; i++) {
      histogramSB.append(" #").append(i).append(" ").append((double) i);
    }
    histogramSB.append(" TestMetric source=Test key=value");

    decoder.decode(histogramSB.toString(), out, "customer", ingesterContext);

    assertThat(out).isNotEmpty();
    ReportHistogram p = out.get(0);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = p.getValue();

    // Verify we have less centroids after compression.
    assertThat(centroidsLimit).isGreaterThan(h.getBins().size());
  }
}
