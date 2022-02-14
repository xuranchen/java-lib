package com.wavefront.data;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.ingester.ReportMetricDecoder;
import com.wavefront.ingester.ReportHistogramDecoder;
import com.wavefront.ingester.SpanDecoder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import wavefront.report.Annotation;
import wavefront.report.ReportHistogram;
import wavefront.report.ReportMetric;
import wavefront.report.Span;
import wavefront.report.SpanLogs;
import wavefront.report.ReportLog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;

/**
 * @author vasily@wavefront.com
 */
public class ValidationTest {

  private ValidationConfiguration config;
  private ReportMetricDecoder decoder;

  @Before
  public void testSetup() {
    this.decoder = new ReportMetricDecoder(null, ImmutableList.of());
    this.config = new ValidationConfiguration().
        setMetricLengthLimit(15).
        setHostLengthLimit(10).
        setHistogramLengthLimit(10).
        setSpanLengthLimit(20).
        setAnnotationsCountLimit(4).
        setAnnotationsKeyLengthLimit(5).
        setAnnotationsValueLengthLimit(10).
        setSpanAnnotationsCountLimit(3).
        setSpanAnnotationsKeyLengthLimit(16).
        setSpanAnnotationsValueLengthLimit(36).
        setLogLengthLimit(100).
        setLogAnnotationsKeyLengthLimit(10).
        setLogAnnotationsValueLengthLimit(10).
        setLogAnnotationsCountLimit(10);
  }

  @Test
  public void testPointIllegalChars() {
    String input = "metric1";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good.metric2";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good-metric3";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good_metric4";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good,metric5";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good/metric6";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can no longer be ~
    input = "~good.metric7";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can be ∆ (\u2206)
    input = "∆delta.metric8";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // second character can be ~ if first character is ∆ (\u2206)
    input = "∆~delta.metric8";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can be Δ (\u0394)
    input = "Δdelta.metric9";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // second character can be ~ if first character is Δ (\u0394)
    input = "Δ~delta.metric9";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // non-first character cannot be ~
    input = "~good.~metric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // non-first character cannot be ∆ (\u2206)
    input = "∆delta.∆metric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // non-first character cannot be Δ (\u0394)
    input = "∆delta.Δmetric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in ~
    input = "good.metric.~";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in ∆ (\u2206)
    input = "delta.metric.∆";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in Δ (\u0394)
    input = "delta.metric.Δ";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ~";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as;df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as:df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as'df";
    Assert.assertFalse(Validation.charactersAreValid(input));
  }

  @Test
  public void testPointAnnotationKeyValidation() {
    List<Annotation> good = ImmutableList.of(new Annotation("key", "value"));
    List<Annotation> bad = ImmutableList.of(new Annotation("k:ey", "value"));

    ReportMetric rp = new ReportMetric("some metric", System.currentTimeMillis(), 10.0, "host",
        "table", good);
    Assert.assertTrue(Validation.annotationKeysAreValid(rp.getAnnotations()));

    rp.setAnnotations(bad);
    Assert.assertFalse(Validation.annotationKeysAreValid(rp.getAnnotations()));
  }

  @Test
  public void testValidationConfig() {
    ReportMetric point = getValidPoint();
    Validation.validateMetric(point, config);

    ReportHistogram histogram = getValidHistogram();
    Validation.validateHistogram(histogram, config);

    Span span = getValidSpan();
    Validation.validateSpan(span, config);
  }

  @Test
  public void testInvalidPointsWithValidationConfig() {
    ReportMetric point = getValidPoint();
    Validation.validateMetric(point, config);

    // metric has invalid characters: WF-400
    point.setMetric("metric78@901234");
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-400"));
    }

    // point tag key has invalid characters: WF-401
    point = getValidPoint();
    point.getAnnotations().removeIf(k -> k.getKey().equals("tagk4"));
    point.getAnnotations().add(new Annotation("tag!4", "value"));
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-401"));
    }

    // delta metrics can't be non-positive: WF-404
    point = getValidPoint();
    point.setMetric("∆delta");
    point.setValue(1.0d);
    Validation.validateMetric(point, config);
    point.setValue(1L);
    Validation.validateMetric(point, config);
    point.setValue(-0.1d);
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (DeltaCounterValueException iae) {
      assertTrue(iae.getMessage().contains("WF-404"));
    }
    point.setValue(0.0d);
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-404"));
    }
    point.setValue(-1L);
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-404"));
    }

    // empty histogram: WF-405
    ReportHistogram histo = getValidHistogram();
    Validation.validateHistogram(histo, config);

    histo.getValue().setCounts(ImmutableList.of(0, 0, 0));
    try {
      Validation.validateHistogram(histo, config);
      fail();
    } catch (EmptyHistogramException iae) {
      assertTrue(iae.getMessage().contains("WF-405"));
    }
    histo = getValidHistogram();
    histo.getValue().setBins(ImmutableList.of());
    histo.getValue().setCounts(ImmutableList.of());
    try {
      Validation.validateHistogram(histo, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-405"));
    }

    // missing source: WF-406
    point = getValidPoint();
    point.setHost("");
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-406"));
    }

    // source name too long: WF-407
    point = getValidPoint();
    point.setHost("host567890");
    Validation.validateMetric(point, config);
    point = getValidPoint();
    point.setHost("host5678901");
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-407"));
    }

    // metric too long: WF-408
    point = getValidPoint();
    point.setMetric("metric789012345");
    Validation.validateMetric(point, config);
    point = getValidPoint();
    point.setMetric("metric7890123456");
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-408"));
    }

    // histogram name too long: WF-409
    histo = getValidHistogram();
    histo.setMetric("metric7890");
    Validation.validateHistogram(histo, config);
    histo = getValidHistogram();
    histo.setMetric("metric78901");
    try {
      Validation.validateHistogram(histo, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-409"));
    }

    // too many point tags: WF-410
    point = getValidPoint();
    point.getAnnotations().add(new Annotation("newtag", "newtagV"));
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-410"));
    }

    // point tag (key+value) too long: WF-411
    point = getValidPoint();
    point.getAnnotations().removeIf(k -> k.getKey().equals("tagk4"));
    point.getAnnotations().add(new Annotation(Strings.repeat("k", 100), Strings.repeat("v", 154)));
    ValidationConfiguration tagConfig = new ValidationConfiguration().
        setAnnotationsKeyLengthLimit(255).
        setAnnotationsValueLengthLimit(255);
    Validation.validateMetric(point, tagConfig);
    point.getAnnotations().add(new Annotation(Strings.repeat("k", 100), Strings.repeat("v", 155)));
    try {
      Validation.validateMetric(point, tagConfig);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-411"));
    }

    // point tag key too long: WF-412
    point = getValidPoint();
    point.getAnnotations().removeIf(k -> k.getKey().equals("tagk4"));
    point.getAnnotations().add(new Annotation("tagk44", "v"));
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-412"));
    }

    // point tag value too long: WF-413
    point = getValidPoint();
    point.getAnnotations().removeIf(x -> x.getKey().equals("tagk4"));
    point.getAnnotations().add(new Annotation("tagk4", "value67890"));
    Validation.validateMetric(point, config);
    point = getValidPoint();
    point.getAnnotations().removeIf(x -> x.getKey().equals("tagk4"));
    point.getAnnotations().add(new Annotation("tagk4", "value678901"));
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-413"));
    }

    // point tag value empty: WF-414
    point = getValidPoint();
    point.getAnnotations().removeIf(x -> x.getKey().equals("tagk4"));
    point.getAnnotations().add(new Annotation("tagk4", ""));
    try {
      Validation.validateMetric(point, config);
      fail();
    } catch (EmptyTagValueException iae) {
      assertTrue(iae.getMessage().contains("WF-414"));
    }
  }

  @Test(expected = DeltaCounterValueException.class)
  public void testZeroDeltaValue() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(() -> "localhost", ImmutableList.of());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("Δrequest.count 0 source=test.wfc", out);
    Validation.validateMetric(out.get(0), config);
  }

  @Test(expected = DeltaCounterValueException.class)
  public void testNegativeDeltas() throws Exception {
    ReportMetricDecoder decoder = new ReportMetricDecoder(() -> "localhost", ImmutableList.of());
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("∆request.count -1 source=test.wfc", out);
    Validation.validateMetric(out.get(0), config);
  }

  @Test
  public void testInvalidSpansWithValidationConfig() {
    Span span;

    // span annotation key has invalid characters: WF-416
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("$key", "v"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-416"));
    }

    // span missing source: WF-426
    span = getValidSpan();
    span.setSource("");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-426"));
    }

    // span source name too long: WF-427
    span = getValidSpan();
    span.setSource("source7890");
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.setSource("source78901");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-427"));
    }

    // span name too long: WF-428
    span = getValidSpan();
    span.setName("spanName901234567890");
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.setName("spanName9012345678901");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-428"));
    }

    // span has too many annotations: WF-430
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k1", "v1"));
    span.getAnnotations().add(new Annotation("k2", "v2"));
    Validation.validateSpan(span, config);
    span.getAnnotations().add(new Annotation("k3", "v3"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-430"));
    }

    // span annotation key too long: WF-432
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k23456", "v1"));
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k2345678901234567", "v1"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-432"));
    }

    // accept long span annotation value
    List<SpanLogs> spanLogsToReport = new ArrayList<>();
    Consumer<SpanLogs> spanLogsConsumer = spanLogsToReport::add;
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k", "v23456789012345"));
    Validation.validateSpan(span, config, spanLogsConsumer);
    assertEquals("v23456789012345", span.getAnnotations().get(1).getValue());
    assertEquals(0, spanLogsToReport.size());

    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k", "v123456789012345678901234567890123456"));
    span.getAnnotations().add(new Annotation("_spanSecondaryId", "713d14bd-e7c8-415a-b633-4042ba8a11fd"));
    Validation.validateSpan(span, config, spanLogsConsumer);
    System.currentTimeMillis();
    assertEquals("v12345678901234567890123456789012345", span.getAnnotations().get(1).getValue());
    assertEquals("dummy", spanLogsToReport.get(0).getCustomer());
    assertEquals("d5355bf7-fc8d-48d1-b761-75b170f396e0", spanLogsToReport.get(0).getTraceId());
    assertEquals("4217104a-690d-4927-baff-d9aa779414c2", spanLogsToReport.get(0).getSpanId());
    assertEquals("713d14bd-e7c8-415a-b633-4042ba8a11fd", spanLogsToReport.get(0).getSpanSecondaryId());
    assertNull(spanLogsToReport.get(0).getSpan());
    assertEquals("v123456789012345678901234567890123456",
            spanLogsToReport.get(0).getLogs().get(0).getFields().get("k"));
    assertEquals(-1, spanLogsToReport.get(0).getLogs().get(0).getTimestamp());
    assertEquals(AnnotationUtils.getValue(span.getAnnotations(), "_spanLogs"), Boolean.toString(true));

    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k", "v123456789012345678901234567890123456"));
    span.getAnnotations().add(new Annotation("_spanLogs", Boolean.toString(false)));
    Validation.validateSpan(span, config, spanLogsConsumer);
    assertEquals(AnnotationUtils.getValue(span.getAnnotations(), "_spanLogs"), Boolean.toString(true));
  }

  @Test
  public void testValidLog() {
    Validation.validateLog(getValidLog(), config);
  }

  @Test
  public void testInvalidLog() {
    // Test Null Source
    ReportLog nullHostLog = getValidLog();
    nullHostLog.setHost("");
    Exception e = assertThrows(DataValidationException.class, () -> Validation.validateLog(nullHostLog, config));
    assertEquals(Validation.LOG_SOURCE_REQUIRED_ERROR, e.getMessage());

    // Test Host Too Long
    ReportLog hostTooLongLog = getValidLog();
    StringBuilder hostTooLong = new StringBuilder();
    for (int i = 0; i < config.getHostLengthLimit() + 1; i++) {
      hostTooLong.append("a");
    }
    hostTooLongLog.setHost(String.valueOf(hostTooLong));
    e = assertThrows(DataValidationException.class, () -> Validation.validateLog(hostTooLongLog, config));
    String errorMsg = String.format(Validation.LOG_SOURCE_TOO_LONG_ERROR, (config.getHostLengthLimit() + 1)
            , config.getHostLengthLimit(), hostTooLong);
    assertEquals(errorMsg, e.getMessage());

    // Test Log Message Too Long
    ReportLog messageTooLongLog = getValidLog();
    StringBuilder stringTooLong = new StringBuilder();
    for (int i = 0; i < config.getLogLengthLimit() + 1; i++) {
      stringTooLong.append("a");
    }
    messageTooLongLog.setMessage(String.valueOf(stringTooLong));
    e = assertThrows(DataValidationException.class, () -> Validation.validateLog(messageTooLongLog, config));
    errorMsg = String.format(Validation.LOG_MESSAGE_TOO_LONG_ERROR, config.getLogLengthLimit() + 1
            , config.getLogLengthLimit(), messageTooLongLog.getMessage());
    assertEquals(errorMsg, e.getMessage());

    // Test Too Many Annotations
    ReportLog tooManyAnnotationsLog = getValidLog();
    List<Annotation> annotationList = new ArrayList<>();
    for (int i = 0; i < config.getLogAnnotationsCountLimit() + 1; i++) {
      annotationList.add(new Annotation());
    }
    tooManyAnnotationsLog.setAnnotations(annotationList);
    e = assertThrows(DataValidationException.class, () -> Validation.validateLog(tooManyAnnotationsLog, config));
    errorMsg = String.format(Validation.LOG_TOO_MANY_ANNOTATIONS_ERROR, (config.getLogAnnotationsCountLimit() + 1)
            , config.getLogAnnotationsCountLimit());
    assertEquals(errorMsg, e.getMessage());

    // Test annotation Tag Key Too long
    ReportLog annotationKeyTooLongLog = getValidLog();
    annotationList = new ArrayList<>();
    stringTooLong = new StringBuilder();
    for (int i = 0; i < config.getLogAnnotationsKeyLengthLimit() + 1; i++) {
      stringTooLong.append("a");
    }
    annotationList.add(new Annotation(String.valueOf(stringTooLong), "mValue"));
    annotationKeyTooLongLog.setAnnotations(annotationList);
    e = assertThrows(DataValidationException.class, () -> Validation.validateLog(annotationKeyTooLongLog, config));
    errorMsg = String.format(Validation.LOG_TAG_KEY_TOO_LONG_ERROR, (config.getLogAnnotationsKeyLengthLimit() + 1)
            , config.getLogAnnotationsValueLengthLimit(), stringTooLong);
    assertEquals(errorMsg, e.getMessage());

    // Test invalid characters in Annotation Key
    ReportLog invalidAnnotationKeyLog = getValidLog();
    annotationList = new ArrayList<>();
    String invalidKey = "!@#$%^^&*(";
    annotationList.add(new Annotation(invalidKey, "mValue"));
    invalidAnnotationKeyLog.setAnnotations(annotationList);
    e = assertThrows(DataValidationException.class, () -> Validation.validateLog(invalidAnnotationKeyLog, config));
    errorMsg = String.format(Validation.LOG_TAG_KEY_ILLEGAL_CHAR_ERROR, invalidKey);
    assertEquals(errorMsg, e.getMessage());

    // Test blank Annotation value
    ReportLog blankAnnotationValueLog = getValidLog();
    annotationList = new ArrayList<>();
    annotationList.add(new Annotation("mKey", ""));
    blankAnnotationValueLog.setAnnotations(annotationList);
    e = assertThrows(EmptyTagValueException.class, () -> Validation.validateLog(blankAnnotationValueLog, config));
    errorMsg = String.format(Validation.LOG_ANNOTATION_NO_VALUE_ERROR, "mKey");
    assertEquals(errorMsg, e.getMessage());

    // Test annotation value Too long
    ReportLog annotationValueTooLongLog = getValidLog();
    annotationList = new ArrayList<>();
    stringTooLong = new StringBuilder();
    for (int i = 0; i < config.getLogAnnotationsValueLengthLimit() + 1; i++) {
      stringTooLong.append("a");
    }
    annotationList.add(new Annotation("mKey", String.valueOf(stringTooLong)));
    annotationValueTooLongLog.setAnnotations(annotationList);
    e = assertThrows(DataValidationException.class, () -> Validation.validateLog(annotationValueTooLongLog, config));
    errorMsg = String.format(Validation.LOG_ANNOTATION_VALUE_TOO_LONG_ERROR,
            (config.getLogAnnotationsValueLengthLimit() + 1), config.getLogAnnotationsValueLengthLimit(), stringTooLong);
    assertEquals(errorMsg, e.getMessage());

  }

  @Test
  public void testValidHistogram() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();
    decoder.decode("!M 1533849540 #1 0.0 #2 1.0 #3 3.0 TestMetric source=Test key=value", out, 
        "dummy");
    Validation.validateHistogram(out.get(0), new ValidationConfiguration());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyHistogramThrows() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();
    decoder.decode("!M 1533849540 #0 0.0 #0 1.0 #0 3.0 TestMetric source=Test key=value", out, "dummy");
    Validation.validateHistogram(out.get(0), new ValidationConfiguration());
    Assert.fail("Empty Histogram should fail validation!");
  }

  private ReportMetric getValidPoint() {
    List<ReportMetric> out = new ArrayList<>();
    decoder.decode("metric789012345 1 source=source7890 tagk1=tagv1 tagk2=tagv2 tagk3=tagv3 tagk4=tagv4",
        out, "dummy");
    return out.get(0);
  }

  private ReportHistogram getValidHistogram() {
    ReportHistogramDecoder decoder = new ReportHistogramDecoder();
    List<ReportHistogram> out = new ArrayList<>();
    decoder.decode("!M 1533849540 #1 0.0 #2 1.0 #3 3.0 TestMetric source=Test key=value", out, "dummy");
    return out.get(0);
  }

  private Span getValidSpan() {
    List<Span> spanOut = new ArrayList<>();
    SpanDecoder spanDecoder = new SpanDecoder("default");
    spanDecoder.decode("testSpanName source=spanSource spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
            "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 tagkey=tagvalue1 1532012145123456 1532012146234567 ",
        spanOut);
    return spanOut.get(0);
  }

  private ReportLog getValidLog() {
    ReportLog log = new ReportLog();

    long timeStamp = System.currentTimeMillis();
    log.setTimestamp(timeStamp);

    String validSource = "myHost";
    log.setHost(validSource);

    String message = "oh no an error";
    log.setMessage(message);

    Annotation annotation = new Annotation("mKey", "mValue");
    List<Annotation> annotationList = new ArrayList<>();
    annotationList.add(annotation);
    log.setAnnotations(annotationList);

    return log;
  }
}
