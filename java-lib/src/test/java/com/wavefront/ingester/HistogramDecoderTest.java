package com.wavefront.ingester;

import com.wavefront.common.Clock;
import com.wavefront.data.TooManyCentroidException;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.ingester.IngesterContext.DEFAULT_CENTROIDS_COUNT_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class HistogramDecoderTest {

  @Test
  public void testBasicMessage() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 1471988653 #3 123.237 TestMetric source=Test key=value", out, "customer",
        new IngesterContext.Builder().withTargetHistogramAccuracy(32).throwIfTooManyHistogramCentroids(10).build());

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
    assertThat(p.getMetric()).isEqualTo("TestMetric");
    // Should be converted to Millis and pinned to the beginning of the corresponding minute
    assertThat(p.getTimestamp()).isEqualTo(1471988640000L);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    assertThat(p.getHost()).isEqualTo("Test");
    assertThat(p.getTable()).isEqualTo("customer");
    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).containsEntry("key", "value");

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
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!H 1471988653 #3 123.237 TestMetric source=Test key=value", out, "customer");

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
    // Should be converted to Millis and pinned to the beginning of the corresponding hour
    assertThat(p.getTimestamp()).isEqualTo(1471986000000L);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_HOUR);
  }

  @Test
  public void testDayBin() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!D 1471988653 #3 123.237 TestMetric source=Test key=value", out, "customer");

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
    // Should be converted to Millis and pinned to the beginning of the corresponding day
    assertThat(p.getTimestamp()).isEqualTo(1471910400000L);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    assertThat(h.getDuration()).isEqualTo(DateUtils.MILLIS_PER_DAY);
  }

  @Test
  public void testTagKey() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 1471988653 #3 123.237 TestMetric source=Test tag=value", out, "customer");

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);

    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).containsEntry("_tag", "value");
  }

  @Test
  public void testMultipleBuckets() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 1471988653 #1 3.1416 #1 2.7183 TestMetric", out, "customer");

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
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
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 1471988653 #1 -3.1416 TestMetric", out, "customer");

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
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
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("1471988653 #3 123.237 TestMetric source=Test tag=value", out, "customer");
  }

  @Test
  public void testMissingTimestamp() {
    //Note - missingTimestamp to port 40,000 is no longer invalid - see MONIT-6430 for more details
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #3 123.237 TestMetric source=Test tag=value", out, "customer");

    assertThat(out).isNotEmpty();
    long expectedTimestamp = Clock.now();

    ReportPoint p = out.get(0);
    assertThat(p.getMetric()).isEqualTo("TestMetric");

    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    // Should be converted to Millis and pinned to the beginning of the corresponding minute
    long duration = ((Histogram) p.getValue()).getDuration();
    expectedTimestamp = (expectedTimestamp / duration) * duration;
    assertThat(p.getTimestamp()).isEqualTo(expectedTimestamp);

    assertThat(p.getHost()).isEqualTo("Test");
    assertThat(p.getTable()).isEqualTo("customer");
    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).containsEntry("_tag", "value");

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
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 1471988653 TestMetric source=Test tag=value", out, "customer");
  }

  @Test
  public void testMissingMean() {
    //Note - missingTimestamp to port 40,000 is no longer invalid - see MONIT-6430 for more details
    // as a side-effect of that, this test no longer fails!!!
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #3 1471988653 TestMetric source=Test tag=value", out, "customer");

    assertThat(out).isNotEmpty();
    long expectedTimestamp = Clock.now();

    ReportPoint p = out.get(0);
    assertThat(p.getMetric()).isEqualTo("TestMetric");

    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    // Should be converted to Millis and pinned to the beginning of the corresponding minute
    long duration = ((Histogram) p.getValue()).getDuration();
    expectedTimestamp = (expectedTimestamp / duration) * duration;
    assertThat(p.getTimestamp()).isEqualTo(expectedTimestamp);

    assertThat(p.getHost()).isEqualTo("Test");
    assertThat(p.getTable()).isEqualTo("customer");
    assertThat(p.getAnnotations()).isNotNull();
    assertThat(p.getAnnotations()).containsEntry("_tag", "value");

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
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 3.412 1471988653 TestMetric source=Test tag=value", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testZeroCount() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #0 3.412 1471988653 TestMetric source=Test tag=value", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testMissingMetric() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("1471988653 #3 123.237 source=Test tag=value", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testMissingCentroid() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #1 TestMetric source=Test", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testMissingCentroid2() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #1 12345 #2 TestMetric source=Test", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testTooManyCentroids() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #1 12345 #2 123453 1234534 12334 TestMetric source=Test", out, "customer");
  }

  @Test(expected = RuntimeException.class)
  public void testNoCentroids() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M 12334 TestMetric source=Test", out, "customer");
  }

  @Test
  public void testNoSourceAnnotationsIsNotNull() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    decoder.decodeReportPoints("!M #1 12334 TestMetric", out, "customer");

    ReportPoint p = out.get(0);
    assertNotNull(p.getAnnotations());
  }

  @Test
  public void testDefaultThrowTooManyCentroidsException() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

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
    decoder.decodeReportPoints(histogramSB.toString(), out, "customer", ingesterContext);
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
      decoder.decodeReportPoints(histogramSB.toString(), out, "customer", ingesterContext);
      fail();
    } catch (TooManyCentroidException e) {
      // OK
    }
  }

  @Test(expected = TooManyCentroidException.class)
  public void testCustomThrowTooManyCentroidsException() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

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

    decoder.decodeReportPoints(histogramSB.toString(), out, "customer", ingesterContext);
  }

  @Test
  public void testHistogramOptimization() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    IngesterContext ingesterContext = new IngesterContext.Builder().withTargetHistogramAccuracy(8).
        withOptimizeHistograms(true).build();

    int centroidsLimit = 50;
    StringBuilder histogramSB = new StringBuilder();
    histogramSB.append("!M 1471988653");
    for (int i = 1; i < centroidsLimit + 1; i++) {
      histogramSB.append(" #").append(i).append(" ").append((double) i);
    }
    histogramSB.append(" TestMetric source=Test key=value");

    decoder.decodeReportPoints(histogramSB.toString(), out, "customer", ingesterContext);

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    // Verify we have less centroids after compression.
    assertThat(centroidsLimit).isGreaterThan(h.getBins().size());
  }

  @Test
  public void testMultipleRewrites() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();

    IngesterContext ingesterContext =
            new IngesterContext.Builder().withTargetHistogramAccuracy(32).
                    throwIfTooManyHistogramCentroids(100).withOptimizeHistograms(true).build();

    String s = "!M 1471988653 #16456 1591 #747 2 #8436 81 #12418 104 #13033 1496 #7012 112583 #811 298964 #4922 46 " +
            "#10531 1056 #1248 1875 #1340 297186 #18784 424 #12377 3328 #4050 276614 #6082 27 #16895 1273 #6054 " +
            "244180 #15411 191 #17177 1096 #4467 270371 #9570 62 #17097 1223 #6037 234635 #18607 437 #7627 36 #13492 " +
            "171 #18478 1156 #10438 1803 #281 4 #9509 119 #16272 792 #2001 1137 #6601 224814 #7985 51 #11230 161 " +
            "#17005 364 #13822 2037 #9733 125256 #6459 31 #6788 68 #12295 72705 #1707 3 #14993 387 #14178 1845 #13144 " +
            "35250 #17580 376 #17257 398 #10746 1453 #13539 14069 #3645 24 #16091 250 #12815 260 #17397 280 #7425 " +
            "191493 #605 265747 #11772 1323 #3008 287442 #1237 1 #10035 111 #14229 145 #17960 316 #7964 42 #16197 " +
            "1540 #9629 140006 #9235 1688 #14510 1719 #2227 4 #3373 8 #16455 304 #15426 340 #4502 21 #9764 155555 " +
            "#11299 155 #3054 1213 #15864 1643 #5354 261841 #254 299646 #12264 125 #11590 200 #16778 239 #13997 1766 " +
            "#2477 291390 #1868 294842 #10653 87 #13820 136 #15529 1368 #15642 1414 #4466 14 #18079 352 #17426 451 " +
            "#3244 282363 #593 10 #8006 56 #3946 11 #18072 504 #9261 169634 #11343 95 #8404 100533 #6378 214476 " +
            "TestMetric source=Test key=value";

    decoder.decodeReportPoints(s, out, "customer", ingesterContext);

    assertThat(out).isNotEmpty();
    ReportPoint p = out.get(0);
    assertThat(p.getValue()).isNotNull();
    assertThat(p.getValue().getClass()).isEqualTo(Histogram.class);

    Histogram h = (Histogram) p.getValue();

    // Verify we have less centroids after compression.
    assertThat(99).isGreaterThan(h.getBins().size());
  }
}
