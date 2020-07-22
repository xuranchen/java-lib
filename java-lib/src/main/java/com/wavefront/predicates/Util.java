package com.wavefront.predicates;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mdimension.jchronic.Chronic;
import com.mdimension.jchronic.Options;

import wavefront.report.Annotation;
import wavefront.report.ReportHistogram;
import wavefront.report.ReportMetric;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.wavefront.ingester.AbstractIngesterFormatter.unquote;

/**
 * Miscellaneous utility methods used by parsers and preprocessors.
 *
 * @author vasily@wavefront.com.
 */
public abstract class Util {
  private static final Pattern PLACEHOLDERS = Pattern.compile("\\{\\{(.*?)}}");

  private Util() {
  }

  /**
   * Parse a natural language time interval expression (e.g. "now", "10 minutes ago", etc)
   *
   * @param interval   time interval to parse
   * @param anchorTime anchor time in epoch millis
   * @param timeZone   time zone
   * @return epoch millis
   */
  public static long parseTextualTimeExact(String interval, long anchorTime, TimeZone timeZone) {
    Calendar instance = Calendar.getInstance();
    instance.setTimeZone(timeZone);
    instance.setTimeInMillis(anchorTime);
    com.mdimension.jchronic.utils.Span parse = Chronic.parse(unquote(interval),
        new Options(instance));
    if (parse == null) {
      throw new IllegalArgumentException("Failed to parse " + interval + " as a time-expression");
    }
    return parse.getBeginCalendar().getTimeInMillis();
  }

  /**
   * Substitute {{...}} placeholders with corresponding components of the point
   * {{metricName}} {{sourceName}} are replaced with the metric name and source respectively
   * {{anyTagK}} is replaced with the value of the anyTagK point tag
   *
   * @param input        input string with {{...}} placeholders
   * @param reportPoint  ReportPoint object to extract components from
   * @return string with substituted placeholders
   */
  @Deprecated
  public static String expandPlaceholders(String input, ReportPoint reportPoint) {
    if (reportPoint != null && input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = PLACEHOLDERS.matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "metricName":
              substitution = reportPoint.getMetric();
              break;
            case "sourceName":
              substitution = reportPoint.getHost();
              break;
            default:
              substitution = reportPoint.getAnnotations().get(placeholders.group(1));
          }
          placeholders.appendReplacement(result, firstNonNull(substitution, ""));
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }

  /**
   * Substitute {{...}} placeholders with corresponding components of the point
   * {{metricName}} {{sourceName}} are replaced with the metric name and source respectively
   * {{anyTagK}} is replaced with the value of the anyTagK point tag
   *
   * @param input        input string with {{...}} placeholders
   * @param reportMetric ReportMetric object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, ReportMetric reportMetric) {
    if (reportMetric != null && input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = PLACEHOLDERS.matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "metricName":
              substitution = reportMetric.getMetric();
              break;
            case "sourceName":
              substitution = reportMetric.getHost();
              break;
            default:
              substitution = reportMetric.getAnnotations().stream().
                  filter(a -> a.getKey().equals(placeholders.group(1))).
                  map(Annotation::getValue).findFirst().orElse(null);
          }
          placeholders.appendReplacement(result, firstNonNull(substitution, ""));
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }

  /**
   * Substitute {{...}} placeholders with corresponding components of the histogram
   * {{metricName}} {{sourceName}} are replaced with the metric name and source respectively
   * {{anyTagK}} is replaced with the value of the anyTagK point tag
   *
   * @param input           input string with {{...}} placeholders
   * @param reportHistogram ReportHistogram object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, ReportHistogram reportHistogram) {
    if (reportHistogram != null && input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = PLACEHOLDERS.matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "metricName":
              substitution = reportHistogram.getMetric();
              break;
            case "sourceName":
              substitution = reportHistogram.getHost();
              break;
            default:
              substitution = reportHistogram.getAnnotations().stream().
                  filter(a -> a.getKey().equals(placeholders.group(1))).
                  map(Annotation::getValue).findFirst().orElse(null);
          }
          placeholders.appendReplacement(result, firstNonNull(substitution, ""));
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }

  /**
   * Substitute {{...}} placeholders with corresponding components of a Span
   * {{spanName}} {{sourceName}} are replaced with the span name and source respectively
   * {{anyKey}} is replaced with the value of an annotation with anyKey key
   *
   * @param input input string with {{...}} placeholders
   * @param span  Span object to extract components from
   * @return string with substituted placeholders
   */
  public static String expandPlaceholders(String input, Span span) {
    if (span != null && input.contains("{{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = PLACEHOLDERS.matcher(input);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          String substitution;
          switch (placeholders.group(1)) {
            case "spanName":
              substitution = span.getName();
              break;
            case "sourceName":
              substitution = span.getSource();
              break;
            default:
              substitution = span.getAnnotations().stream().
                  filter(a -> a.getKey().equals(placeholders.group(1))).
                  map(Annotation::getValue).findFirst().orElse(null);
          }
          placeholders.appendReplacement(result, firstNonNull(substitution, ""));
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return input;
  }
}
