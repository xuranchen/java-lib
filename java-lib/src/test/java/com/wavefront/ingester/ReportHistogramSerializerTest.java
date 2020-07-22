package com.wavefront.ingester;

import java.util.function.Function;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportHistogram;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Andrew Kao (andrew@wavefront.com), Jason Bau (jbau@wavefront.com), vasily@wavefront.com
 */
public class ReportHistogramSerializerTest {
  private ReportHistogram histogramPoint;

  private Function<ReportHistogram, String> serializer = new ReportHistogramSerializer();

  @Before
  public void setUp() {
    Histogram h = Histogram.newBuilder()
        .setType(HistogramType.TDIGEST)
        .setBins(ImmutableList.of(10D, 10D, 20D, 20D))
        .setCounts(ImmutableList.of(1, 1, 3, 1))
        .setDuration((int) DateUtils.MILLIS_PER_MINUTE)
        .build();
    histogramPoint = ReportHistogram.newBuilder()
        .setCustomer("customer")
        .setValue(h)
        .setMetric("TestMetric")
        .setHost("TestSource")
        .setTimestamp(1469751813000L)
        .setAnnotations(ImmutableList.of(new Annotation("keyA", "valueA"),
            new Annotation("keyB", "valueB")))
        .build();
  }

  @Test
  public void testHistogramReportHistogramToString() {
    String subject = serializer.apply(histogramPoint);

    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"TestMetric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test(expected = RuntimeException.class)
  public void testHistogramReportHistogramToString_unsupportedDuration() {
    histogramPoint.getValue().setDuration(13);
    serializer.apply(histogramPoint);
  }

  @Test
  public void testHistogramReportHistogramToString_binCountMismatch() {
    histogramPoint.getValue().setCounts(ImmutableList.of(10));
    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #10 10.0 \"TestMetric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test
  public void testHistogramReportHistogramToString_quotesInMetric() {
    histogramPoint.setMetric("Test\"Metric");
    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"Test\\\"Metric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test
  public void testHistogramReportHistogramToString_quotesInTags() {
    histogramPoint.setAnnotations(ImmutableList.of(new Annotation("K\"ey", "V\"alue")));
    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"TestMetric\" source=\"TestSource\" \"K\\\"ey\"=\"V\\\"alue\"");
  }
}
