package com.wavefront.predicates;

import javax.annotation.Nullable;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportEvent;
import wavefront.report.ReportHistogram;
import wavefront.report.ReportMetric;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.predicates.Predicates.parsePredicateEvalExpression;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class PredicateEvalExpressionTest {

  private final ReportPoint point = ReportPoint.newBuilder().
      setTable("test").
      setValue(1234.5).
      setTimestamp(1592837162000L).
      setMetric("testMetric").
      setHost("testHost").
      setAnnotations(ImmutableMap.of("tagk1", "tagv1", "tagk2", "tagv2",
          "env", "prod", "dc", "us-west-2")).
      build();
  private final ReportMetric metric = ReportMetric.newBuilder().
      setCustomer("test").
      setValue(1234.5).
      setTimestamp(1592837162000L).
      setMetric("testMetric").
      setHost("testHost").
      setAnnotations(ImmutableList.of(
          new Annotation("tagk1", "tagv1"), new Annotation("tagk2", "tagv2"),
          new Annotation("env", "prod"), new Annotation("dc", "us-west-2"))).
      build();
  private final ReportHistogram histogram = ReportHistogram.newBuilder().
      setCustomer("test").
      setValue(Histogram.newBuilder().setBins(ImmutableList.of(1.0)).
          setCounts(ImmutableList.of(1)).setDuration(60).setType(HistogramType.TDIGEST).build()).
      setTimestamp(1592837162000L).
      setMetric("testMetric").
      setHost("testHost").
      setAnnotations(ImmutableList.of(
          new Annotation("tagk1", "tagv1"), new Annotation("tagk2", "tagv2"),
          new Annotation("env", "prod"), new Annotation("dc", "us-west-2"))).
      build();
  private final Span span = Span.newBuilder().
      setCustomer("test").
      setName("testSpanName").
      setSource("spanSourceName").
      setSpanId("4217104a-690d-4927-baff-d9aa779414c2").
      setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0").
      setAnnotations(ImmutableList.of(
          new Annotation("foo", "bar1-baz"),
          new Annotation("foo", "bar2-baz"),
          new Annotation("boo", "baz"),
          new Annotation("span.kind", "error"),
          new Annotation("http.status_code", "404"),
          new Annotation("application", "beachshirts"))).
      setStartMillis(1532012145123L).
      setDuration(1111).
      build();
  private final ReportEvent event = ReportEvent.newBuilder().setName("test").
      setStartTime(System.currentTimeMillis()).
      setEndTime(System.currentTimeMillis() + 1).
      setAnnotations(ImmutableMap.of()).
      build();

  @Test
  public void testAsPredicate() {
    assertTrue(Predicates.fromPredicateEvalExpression("$value = 1234.5").test(point));
    assertFalse(Predicates.fromPredicateEvalExpression("$value != 1234.5").test(point));
    assertTrue(Predicates.fromPredicateEvalExpression("$value = 1234.5").test(metric));
    assertFalse(Predicates.fromPredicateEvalExpression("$value != 1234.5").test(metric));
    assertTrue(Predicates.fromPredicateEvalExpression("$duration = 1111").test(span));
    assertFalse(Predicates.fromPredicateEvalExpression("$duration != 1111").test(span));
  }

  @Test
  public void testMath() {
    parseAndAssertEq(4, "2 + 2", null);
    parseAndAssertEq(6, "2 * 3", null);
    parseAndAssertEq(1, "3 - 2", null);
    parseAndAssertEq(2.5, "10 / 4", null);
    parseAndAssertEq(1.5, "10.5 % 3", null);
    parseAndAssertEq(0x8800, "0x8000 + 0x800", null);
    parseAndAssertEq(0x8888, "0x8080 | 0x808", null);
    parseAndAssertEq(0, "0x8080 & 0x808", null);
    parseAndAssertEq(0xFF0F, "0xF0F0 ^ 0xFFF", null);
    parseAndAssertEq(0xFFFFF765, "~0x89A", null);
    parseAndAssertEq(5, "10 >> 1", null);
    parseAndAssertEq(-5, "-10 >> 1", null);
    parseAndAssertEq(5, "10 >>> 1", null);
    parseAndAssertEq(9223372036854775803L, "-10 >>> 1", null);
    parseAndAssertEq(20, "5 << 2", null);
    parseAndAssertEq(-40, "-10 << 2", null);
    parseAndAssertEq(20, "5 <<< 2", null);
    parseAndAssertEq(-40, "-10 <<< 2", null);
    parseAndAssertEq(5, "10 >>> 1", null);
    parseAndAssertEq(6, "2 + 2 * 2", null);
    parseAndAssertEq(6, "2 + (2 * 2)", null);
    parseAndAssertEq(8, "(2 + 2) * 2", null);
    parseAndAssertEq(0, "1 = 2", null);
    parseAndAssertEq(1, "1 = 1", null);
    parseAndAssertEq(1, "1 != 2", null);
    parseAndAssertEq(0, "1 != 1", null);
    parseAndAssertEq(0, "2 + 2 = 5", null);
    parseAndAssertEq(1, "2 + 2 = 4", null);
    parseAndAssertEq(1, "2 + 2 > 3", null);
    parseAndAssertEq(1, "2 + 2 >= 4", null);
    parseAndAssertEq(1, "2 + 2 <= 4", null);
    parseAndAssertEq(0, "2 + 2 > 4", null);
    parseAndAssertEq(1, "3 < 2 + 2 < 5", null);
    parseAndAssertEq(0, "4 < 2 + 2", null);
    parseAndAssertEq(1, "(1 = 1) and (2 = 2)", null);
    parseAndAssertEq(0, "(1 = 1) and (2 != 2)", null);
    parseAndAssertEq(0, "(1 != 1) and (2 = 2)", null);
    parseAndAssertEq(0, "(1 != 1) and (2 != 2)", null);
    parseAndAssertEq(1, "(1 = 1) or (2 = 2)", null);
    parseAndAssertEq(1, "(1 = 1) or (2 != 2)", null);
    parseAndAssertEq(1, "(1 != 1) or (2 = 2)", null);
    parseAndAssertEq(1, "5 and 2", null);
    parseAndAssertEq(0, "5 and 0", null);
    parseAndAssertEq(0, "0 and 5", null);
    parseAndAssertEq(0, "0 and 0", null);
    parseAndAssertEq(1, "5 or 2", null);
    parseAndAssertEq(1, "5 or 0", null);
    parseAndAssertEq(1, "0 or 5", null);
    parseAndAssertEq(0, "0 or 0", null);
    parseAndAssertEq(1, "not 0", null);
    parseAndAssertEq(0, "not 1", null);
    parseAndAssertEq(0, "not 5", null);
    parseAndAssertEq(1, "1 = 1 and 0 = 0", null);
    parseAndAssertEq(1, "1 = 2 and 0 = 0 or 2 = 2 ", null);
    parseAndAssertEq(1, "1 = 2 or 0 = 0 and 2 = 2 ", null);
    parseAndAssertEq(1, "0 and 1 or 1 ", null);
    parseAndAssertEq(1, "0 or 1 and 1 ", null);
    parseAndAssertEq(0, "0 and 1 or 0 ", null);
    parseAndAssertEq(1, "1 and not 0", null);
    parseAndAssertEq(0, "1 and not 1", null);
    parseAndAssertEq(1, "1 and not 0 or 0", null);
    parseAndAssertEq(1, "1 and not 1 or 1", null);
    parseAndAssertEq(1, "random() > 0", null);
    parseAndAssertEq(1, "random() < 1", null);
  }

  @Test
  public void testUnits() {
    parseAndAssertEq(0.000000000000000000000005, "5y", null);
    parseAndAssertEq(0.000000000000000000005, "5z", null);
    parseAndAssertEq(0.000000000000000005, "5a", null);
    parseAndAssertEq(0.000000000000005, "5f", null);
    parseAndAssertEq(0.000000000005, "5p", null);
    parseAndAssertEq(0.000000005, "5n", null);
    parseAndAssertEq(0.000005, "5µ", null);
    parseAndAssertEq(0.005, "5m", null);
    parseAndAssertEq(0.05, "5c", null);
    parseAndAssertEq(0.5, "5d", null);
    parseAndAssertEq(50, "5da", null);
    parseAndAssertEq(500, "5h", null);
    parseAndAssertEq(5_000, "5k", null);
    parseAndAssertEq(5_000_000, "5M", null);
    parseAndAssertEq(5_000_000_000L, "5G", null);
    parseAndAssertEq(5_000_000_000_000L, "5T", null);
    parseAndAssertEq(5_000_000_000_000_000L, "5P", null);
    parseAndAssertEq(5_000_000_000_000_000_000L, "5E", null);
    parseAndAssertEq(5e21, "5Z", null);
    assertEquals(5e24, parsePredicateEvalExpression("5Y").getValue(null), 1.1e9);
  }

  @Test
  public void testStringComparison() {
    parseAndAssertEq(1, "'aaa' = 'aaa'", null);
    parseAndAssertEq(0, "'aaa' = 'aa'", null);
    parseAndAssertEq(1, "'aaa' equals 'aaa'", null);
    parseAndAssertEq(0, "'aaa' equals 'aa'", null);
    parseAndAssertEq(1, "'aAa' equalsIgnoreCase 'AaA'", null);
    parseAndAssertEq(0, "'aAa' equalsIgnoreCase 'Aa'", null);
    parseAndAssertEq(1, "'aaa' contains 'aa'", null);
    parseAndAssertEq(0, "'aaa' contains 'ab'", null);
    parseAndAssertEq(1, "'aAa' containsIgnoreCase 'aa'", null);
    parseAndAssertEq(1, "'aAa' containsIgnoreCase 'AA'", null);
    parseAndAssertEq(0, "'aAa' containsIgnoreCase 'ab'", null);
    parseAndAssertEq(1, "'abcd' startsWith 'ab'", null);
    parseAndAssertEq(0, "'abcd' startsWith 'cd'", null);
    parseAndAssertEq(1, "'aBCd' startsWithIgnoreCase 'Ab'", null);
    parseAndAssertEq(0, "'aBCd' startsWithIgnoreCase 'Cd'", null);
    parseAndAssertEq(1, "'abcd' endsWith 'cd'", null);
    parseAndAssertEq(0, "'abcd' endsWith 'ab'", null);
    parseAndAssertEq(1, "'aBCd' endsWithIgnoreCase 'cD'", null);
    parseAndAssertEq(0, "'aBCd' endsWithIgnoreCase 'aB'", null);
    parseAndAssertEq(1, "'abcde' matches 'a*e'", null);
    parseAndAssertEq(1, "'abcde' matches '*cde'", null);
    parseAndAssertEq(1, "'abcde' matches 'ab*'", null);
    parseAndAssertEq(1, "'abcde' matches '*bc*'", null);
    parseAndAssertEq(1, "'abcde' matches '*'", null);
    parseAndAssertEq(0, "'abcde' matches '*dc*'", null);
    parseAndAssertEq(0, "'abcde' matches '*z*'", null);
    parseAndAssertEq(1, "'abCDe' matchesIgnoreCase 'a*E'", null);
    parseAndAssertEq(1, "'abCDe' matchesIgnoreCase '*cDe'", null);
    parseAndAssertEq(1, "'abCDe' matchesIgnoreCase 'Ab*'", null);
    parseAndAssertEq(1, "'abCDe' matchesIgnoreCase '*bC*'", null);
    parseAndAssertEq(1, "'abCDe' matchesIgnoreCase '*'", null);
    parseAndAssertEq(0, "'abCDe' matchesIgnoreCase '*DC*'", null);
    parseAndAssertEq(1, "'abcde' regexMatch '^.+bc.+$'", null);
    parseAndAssertEq(0, "'abcde' regexMatch '^.+de.+$'", null);
    parseAndAssertEq(1, "'abCDe' regexMatchIgnoreCase '^.+bc.+$'", null);
    parseAndAssertEq(0, "'abCDe' regexMatchIgnoreCase '^.+de.+$'", null);
    parseAndAssertEq(1, "'bc' in ('ab', 'bc', 'cd')", null);
    parseAndAssertEq(0, "'de' in ('ab', 'bc', 'cd')", null);
  }

  @Test
  public void testStringFunc() {
    parseAndAssertEq(1, "'abc' + 'def' = 'abcdef'", null);
    parseAndAssertEq(1, "'abc' + 'def' = 'ab' + 'cdef'", null);
    parseAndAssertEq(1, "('abc' + 'def') = ('abcdef')", null);
    parseAndAssertEq(1, "'abcdef'.left(3) = 'abc'", null);
    parseAndAssertEq(1, "'abcdef'.right(3) = 'def'", null);
    parseAndAssertEq(1, "'abcdef'.substring(4) = 'ef'", null);
    parseAndAssertEq(1, "'abcdef'.substring(3, 5) = 'de'", null);
    parseAndAssertEq(1, "'aBcDeF'.toLowerCase() = 'abcdef'", null);
    parseAndAssertEq(1, "'aBcDeF'.toUpperCase() = 'ABCDEF'", null);
    parseAndAssertEq(1, "'abcdef'.replace('de', '') = 'abcf'", null);
    parseAndAssertEq(1, "'a1b2c3d4'.replaceAll('\\d', '') = 'abcd'", null);
    parseAndAssertEq(1, "isBlank('')", null);
    parseAndAssertEq(1, "isBlank(' ')", null);
    parseAndAssertEq(0, "isBlank('abc')", null);
    parseAndAssertEq(1, "isEmpty('')", null);
    parseAndAssertEq(0, "isEmpty(' ')", null);
    parseAndAssertEq(0, "isEmpty('abc')", null);
    parseAndAssertEq(0, "isNotBlank('')", null);
    parseAndAssertEq(0, "isNotBlank(' ')", null);
    parseAndAssertEq(1, "isNotBlank('abc')", null);
    parseAndAssertEq(0, "isNotEmpty('')", null);
    parseAndAssertEq(1, "isNotEmpty(' ')", null);
    parseAndAssertEq(1, "isNotEmpty('abc')", null);
    parseAndAssertEq(4, "length('abcd')", null);
    parseAndAssertEq(1139631978, "hashCode('abcd')", null);
    parseAndAssertEq(112566101, "hashCode('utf8-тест')", null);
    parseAndAssertEq(12345, "parse('12345')", null);
    parseAndAssertEq(123, "parse('123a45', 123)", null);
    parseAndAssertEq(0, "parse('123a45')", null);
    parseAndAssertEq(12345, "parse('123a45'.replace('a', ''), 123)", null);
    parseAndAssertEq(1, "str(1) equals '1.0'", null);
    parseAndAssertEq(0, "str(1) equals '1'", null);
    parseAndAssertEq(1, "str(1.9999, '%.2f') equals '2.00'", null);
    parseAndAssertEq(1, "str(1.99, '%.2f') equals '1.99'", null);
    parseAndAssertEq(1, "str(1234.567, '%09.1f') equals '0001234.6'", null);
    parseAndAssertEq(1, "str(12345.67, '%1$,.2f') equals '12,345.67'", null);
    parseAndAssertEq(1, "str(parse('12345') / 100) equals '123.45'", null);
  }

  @Test
  public void testStringEvalFunc() {
    parseAndAssertEq(1, "''.isBlank()", null);
    parseAndAssertEq(1, "' '.isBlank()", null);
    parseAndAssertEq(0, "'abc'.isBlank()", null);
    parseAndAssertEq(1, "''.isEmpty()", null);
    parseAndAssertEq(0, "' '.isEmpty()", null);
    parseAndAssertEq(0, "'abc'.isEmpty()", null);
    parseAndAssertEq(0, "''.isNotBlank()", null);
    parseAndAssertEq(0, "' '.isNotBlank()", null);
    parseAndAssertEq(1, "'abc'.isNotBlank()", null);
    parseAndAssertEq(0, "''.isNotEmpty()", null);
    parseAndAssertEq(1, "' '.isNotEmpty()", null);
    parseAndAssertEq(1, "'abc'.isNotEmpty()", null);
    parseAndAssertEq(4, "'abcd'.length()", null);
    parseAndAssertEq(12345, "'12345'.parse()", null);
    parseAndAssertEq(0, "'12345-a'.parse()", null);
    parseAndAssertEq(123, "'12345-a'.parse(123)", null);
    parseAndAssertEq(1139631978, "'abcd'.hashCode()", null);
  }

  @Test
  public void testIff() {
    parseAndAssertEq(4, "(2 = 2) ? 5 : 6 - 1", null);
    parseAndAssertEq(7, "(2 != 2) ? 5 : 6 + 1", null);
    parseAndAssertEq(5, "if(2 = 2, 5, 6)", null);
    parseAndAssertEq(6, "if(2 != 2, 5, 6)", null);
    parseAndAssertEq(1, "if(2 = 2, 'abc', 'def') = 'abc'", null);
    parseAndAssertEq(1, "if(2 != 2, 'abc', 'def') = 'def'", null);
  }

  @Test
  public void testPointExpression() {
    parseAndAssertEq(1, "$value = 1234.5", point);
    parseAndAssertEq(0, "$value = 1234.0", point);
    parseAndAssertEq(1, "$timestamp = 1592837162000", point);
    parseAndAssertEq(1, "{{sourceName}} contains 'test'", point);
    parseAndAssertEq(1, "'{{sourceName}}' contains 'test'", point);
    parseAndAssertEq(1, "\"{{sourceName}}\" contains 'test'", point);
    parseAndAssertEq(0, "{{sourceName}} contains 'sourceName'", point);
    parseAndAssertEq(1, "{{sourceName}} contains 'sourceName'", null);
    parseAndAssertEq(1, "{{metricName}} contains 'test'", point);
    parseAndAssertEq(1, "{{sourceName}} all startsWith 'test'", point);
    parseAndAssertEq(1, "{{metricName}} all startsWith 'test'", point);
    parseAndAssertEq(1, "{{tagk1}} equals 'tagv1'", point);
    parseAndAssertEq(1, "{{tagk1}} all equals 'tagv1'", point);
    parseAndAssertEq(0, "{{tagk1}} all equals 'tagv1'", null);
    parseAndAssertEq(1, "parse({{tagk2}}.replace('tagv', ''), 3) = 2", point);
    parseAndAssertEq(1, "{{doesNotExist}}.isEmpty()", point);
    parseAndAssertEq(1, "\"{{env}}:{{dc}}\" equals 'prod:us-west-2'", point);
    parseAndAssertEq(1, "$timestamp < time('now')", point);
    parseAndAssertEq(0, "$timestamp > time('31 seconds ago')", point);
    parseAndAssertEq(1, "$timestamp < time('2020-06-23', 'UTC')", point);
  }

  @Test
  public void testMetricExpression() {
    parseAndAssertEq(1, "$value = 1234.5", metric);
    parseAndAssertEq(0, "$value = 1234.0", metric);
    parseAndAssertEq(1, "$timestamp = 1592837162000", metric);
    parseAndAssertEq(1, "{{sourceName}} contains 'test'", metric);
    parseAndAssertEq(1, "'{{sourceName}}' contains 'test'", metric);
    parseAndAssertEq(1, "\"{{sourceName}}\" contains 'test'", metric);
    parseAndAssertEq(0, "{{sourceName}} contains 'sourceName'", metric);
    parseAndAssertEq(1, "{{sourceName}} contains 'sourceName'", null);
    parseAndAssertEq(1, "{{metricName}} contains 'test'", metric);
    parseAndAssertEq(1, "{{sourceName}} all startsWith 'test'", metric);
    parseAndAssertEq(1, "{{metricName}} all startsWith 'test'", metric);
    parseAndAssertEq(1, "{{tagk1}} equals 'tagv1'", metric);
    parseAndAssertEq(1, "{{tagk1}} all equals 'tagv1'", metric);
    parseAndAssertEq(0, "{{tagk1}} all equals 'tagv1'", null);
    parseAndAssertEq(1, "parse({{tagk2}}.replace('tagv', ''), 3) = 2", metric);
    parseAndAssertEq(1, "{{doesNotExist}}.isEmpty()", metric);
    parseAndAssertEq(1, "\"{{env}}:{{dc}}\" equals 'prod:us-west-2'", metric);
    parseAndAssertEq(1, "$timestamp < time('now')", metric);
    parseAndAssertEq(0, "$timestamp > time('31 seconds ago')", metric);
    parseAndAssertEq(1, "$timestamp < time('2020-06-23', 'UTC')", metric);
  }

  @Test
  public void testHistogramExpression() {
    parseAndAssertEq(1, "$timestamp = 1592837162000", histogram);
    parseAndAssertEq(1, "{{sourceName}} contains 'test'", histogram);
    parseAndAssertEq(1, "'{{sourceName}}' contains 'test'", histogram);
    parseAndAssertEq(1, "\"{{sourceName}}\" contains 'test'", histogram);
    parseAndAssertEq(0, "{{sourceName}} contains 'sourceName'", histogram);
    parseAndAssertEq(1, "{{sourceName}} contains 'sourceName'", null);
    parseAndAssertEq(1, "{{metricName}} contains 'test'", histogram);
    parseAndAssertEq(1, "{{sourceName}} all startsWith 'test'", histogram);
    parseAndAssertEq(1, "{{metricName}} all startsWith 'test'", histogram);
    parseAndAssertEq(1, "{{tagk1}} equals 'tagv1'", histogram);
    parseAndAssertEq(1, "{{tagk1}} all equals 'tagv1'", histogram);
    parseAndAssertEq(0, "{{tagk1}} all equals 'tagv1'", null);
    parseAndAssertEq(1, "parse({{tagk2}}.replace('tagv', ''), 3) = 2", histogram);
    parseAndAssertEq(1, "{{doesNotExist}}.isEmpty()", histogram);
    parseAndAssertEq(1, "\"{{env}}:{{dc}}\" equals 'prod:us-west-2'", histogram);
    parseAndAssertEq(1, "$timestamp < time('now')", histogram);
    parseAndAssertEq(0, "$timestamp > time('31 seconds ago')", histogram);
    parseAndAssertEq(1, "$timestamp < time('2020-06-23', 'UTC')", histogram);
  }

  @Test(expected = ExpressionSyntaxException.class)
  public void testTimeWithInvalidStringThrows() {
    parsePredicateEvalExpression("$timestamp > time('NotAValidString')").getValue(point);
  }

  @Test(expected = ExpressionSyntaxException.class)
  public void testUnknownPropertyAccessorThrows() {
    parsePredicateEvalExpression("$unknown > 0").getValue(point);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStartMillisPropertyAccessorThrowsOnPoints() {
    parsePredicateEvalExpression("$startMillis > 0").getValue(point);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDurationPropertyAccessorThrowsOnPoints() {
    parsePredicateEvalExpression("$duration > 0").getValue(point);
  }

  @Test
  public void testSpanExpression() {
    parseAndAssertEq(1, "$duration = 1111", span);
    parseAndAssertEq(0, "$duration = 1111.1", span);
    parseAndAssertEq(1, "$startMillis = 1532012145123", span);
    parseAndAssertEq(1, "{{sourceName}} startsWith 'span'", span);
    parseAndAssertEq(1, "'{{sourceName}}' startsWith 'span'", span);
    parseAndAssertEq(1, "\"{{sourceName}}\" startsWith 'span'", span);
    parseAndAssertEq(0, "{{sourceName}} contains 'sourceName'", span);
    parseAndAssertEq(1, "{{sourceName}} contains 'sourceName'", null);
    parseAndAssertEq(1, "{{spanName}} startsWith 'test'", span);
    parseAndAssertEq(1, "{{foo}} equals 'bar1-baz'", span);
    parseAndAssertEq(1, "{{foo}} all startsWith 'bar'", span);
    parseAndAssertEq(0, "{{foo}} all startsWith 'bar1'", span);
    parseAndAssertEq(1, "{{foo}} any startsWith 'bar1'", span);
    parseAndAssertEq(1, "{{foo}} any startsWith 'bar2'", span);
    parseAndAssertEq(1, "{{foo}} none startsWith 'bar3'", span);
    parseAndAssertEq(1, "{{foo}} all endsWith 'baz'", span);
    parseAndAssertEq(1, "{{foo}} all endsWithIgnoreCase 'BAZ'", span);
    parseAndAssertEq(1, "{{foo}} all startsWithIgnoreCase 'BAR'", span);
    parseAndAssertEq(1, "{{foo}} all contains '-'", span);
    parseAndAssertEq(1, "{{foo}} all containsIgnoreCase '-BA'", span);
    parseAndAssertEq(1, "{{foo}} all matches '*ar*'", span);
    parseAndAssertEq(1, "{{foo}} any matches '*az'", span);
    parseAndAssertEq(0, "{{foo}} any matches '*q*'", span);
    parseAndAssertEq(1, "{{foo}} all matchesIgnoreCase '*AR*'", span);
    parseAndAssertEq(0, "{{foo}} all matches '*ar'", span);
    parseAndAssertEq(0, "{{foo}} all matchesIgnoreCase '*AR'", span);
    parseAndAssertEq(1, "{{foo}} all regexMatch '^.*ar.*$'", span);
    parseAndAssertEq(1, "{{foo}} any regexMatch '^.*az$'", span);
    parseAndAssertEq(0, "{{foo}} any regexMatch '^.*q.*$'", span);
    parseAndAssertEq(1, "{{foo}} all regexMatchIgnoreCase '^.*AR*.$'", span);
    parseAndAssertEq(0, "{{foo}} all regexMatchIgnoreCase '^.*AR$'", span);
    parseAndAssertEq(1, "{{foo}} any equals 'bar2-baz'", span);
    parseAndAssertEq(1, "{{foo}} any equalsIgnoreCase 'bar2-BAZ'", span);
    parseAndAssertEq(1, "{{sourceName}} all startsWith 'span'", span);
    parseAndAssertEq(1, "{{spanName}} all startsWith 'test'", span);
    parseAndAssertEq(1, "{{http.status_code}} in ('400', \"404\")", span);
    parseAndAssertEq(0, "{{span.kind}}='warning'", span);
    parseAndAssertEq(1, "{{application}} = \"beachshirts\" and {{spanName}} startsWith \"test\" " +
        "and {{sourceName}} contains \"span\" and {{http.status_code}} in (\"400\", \"404\")",
        span);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimestampPropertyAccessorThrowsOnSpans() {
    parsePredicateEvalExpression("$timestamp > 0").getValue(span);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValuePropertyAccessorThrowsOnSpans() {
    parsePredicateEvalExpression("$value > 0").getValue(span);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValuePropertyAccessorThrowsOnInvalidObjects() {
    parsePredicateEvalExpression("$value").getValue(event);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTemplateThrowsOnInvalidObjects() {
    parsePredicateEvalExpression("{{tagK}} = '1'").getValue(event);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiStringThrowsOnInvalidObjects() {
    parsePredicateEvalExpression("{{foo}} any matches '*az'").getValue(event);
  }

  @Test
  public void testInvalidObject() {
    // this is ok because we don't need to actually peek into the object itself
    parseAndAssertEq(1, "1", event);
  }

  @Test(expected = ExpressionSyntaxException.class)
  public void testBadSyntax() {
    // this is not an EvalExpression, string expressions can't be evaluated directly
    parsePredicateEvalExpression("{{tagK}}").getValue(null);
  }

  private static void parseAndAssertEq(double d1, String expression, @Nullable Object object) {
    assertEquals(d1, parsePredicateEvalExpression(expression).getValue(object), 1e-12);
  }
}
