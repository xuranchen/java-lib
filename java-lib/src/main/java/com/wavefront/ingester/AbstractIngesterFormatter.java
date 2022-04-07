package com.wavefront.ingester;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wavefront.common.Clock;
import com.wavefront.data.ParseException;

import org.apache.avro.specific.SpecificRecordBase;
import wavefront.report.Annotation;
import wavefront.report.Histogram;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.Arrays;

import static org.apache.commons.lang.StringUtils.containsAny;
import static org.apache.commons.lang.StringUtils.replace;

/**
 * This is the base class for parsing data from plaintext.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 * @author vasily@wavefront.com
 */
public abstract class AbstractIngesterFormatter<T extends SpecificRecordBase> {
  public static final String SOURCE_TAG_LITERAL = "@SourceTag";
  public static final String SOURCE_DESCRIPTION_LITERAL = "@SourceDescription";
  public static final String EVENT_LITERAL = "@Event";

  private static final String SINGLE_QUOTE_STR = "'";
  private static final String ESCAPED_SINGLE_QUOTE_STR = "\\'";
  private static final String DOUBLE_QUOTE_STR = "\"";
  private static final String ESCAPED_DOUBLE_QUOTE_STR = "\\\"";

  private static final List<String> DEFAULT_LOG_MESSAGE_KEYS = Arrays.asList("message", "text");
  private static final List<String> DEFAULT_LOG_TIMESTAMP_KEYS = Arrays.asList("timestamp", "log_timestamp");


  protected final List<FormatterElement<T>> elements;

  protected AbstractIngesterFormatter(List<FormatterElement<T>> elements) {
    this.elements = elements;
  }

  protected interface FormatterElement<T> {
    void consume(StringParser parser, T target);
  }

  /**
   * This class can be used to create a parser for content that proxy receives.
   */
  public abstract static class IngesterFormatBuilder<T extends SpecificRecordBase> {
    final List<FormatterElement<T>> elements = Lists.newArrayList();

    public IngesterFormatBuilder<T> caseSensitiveLiterals(List<String> literals) {
      elements.add(new Text<>(literals, null, true));
      return this;
    }

    public IngesterFormatBuilder<T> caseSensitiveLiterals(List<String> literals,
                                                          BiConsumer<T, String> textConsumer) {
      elements.add(new Text<>(literals, textConsumer, true));
      return this;
    }

    public IngesterFormatBuilder<T> caseInsensitiveLiterals(List<String> literals) {
      elements.add(new Text<>(literals, null, false));
      return this;
    }

    public IngesterFormatBuilder<T> text(BiConsumer<T, String> textConsumer) {
      elements.add(new Text<>(textConsumer));
      return this;
    }

    public IngesterFormatBuilder<T> value(BiConsumer<T, Double> valueConsumer) {
      elements.add(new Value<>(valueConsumer));
      return this;
    }

    public IngesterFormatBuilder<T> centroids() {
      elements.add(new Centroids<>());
      return this;
    }

    public IngesterFormatBuilder<T> timestamp(BiConsumer<T, Long> timestampConsumer) {
      elements.add(new Timestamp<>(timestampConsumer, false, false));
      return this;
    }

    public IngesterFormatBuilder<T> optionalTimestamp(BiConsumer<T, Long> timestampConsumer) {
      elements.add(new Timestamp<>(timestampConsumer, true, false));
      return this;
    }

    public IngesterFormatBuilder<T> rawTimestamp(BiConsumer<T, Long> timestampConsumer) {
      elements.add(new Timestamp<>(timestampConsumer, false, true));
      return this;
    }

    public IngesterFormatBuilder<T> annotationMap(BiConsumer<T, Map<String, String>> mapConsumer) {
      elements.add(new StringMap<>(mapConsumer));
      return this;
    }

    public IngesterFormatBuilder<T> annotationMap(Function<T, Map<String, String>> mapProvider,
                                                  BiConsumer<T, Map<String, String>> mapConsumer) {
      elements.add(new StringMap<>(mapConsumer, mapProvider, null, null));
      return this;
    }

    public IngesterFormatBuilder<T> annotationMap(BiConsumer<T, Map<String, String>> mapConsumer,
                                                  int limit) {
      elements.add(new StringMap<>(mapConsumer, null, limit, null));
      return this;
    }

    public IngesterFormatBuilder<T> annotationList(BiConsumer<T, List<Annotation>> listConsumer) {
      elements.add(new AnnotationList<>(listConsumer, null));
      return this;
    }

    public IngesterFormatBuilder<T> annotationList(BiConsumer<T, List<Annotation>> listConsumer,
                                                   Predicate<String> stringPredicate) {
      elements.add(new AnnotationList<>(listConsumer, stringPredicate));
      return this;
    }

    public IngesterFormatBuilder<T> annotationList(Function<T, List<Annotation>> listProvider,
                                                   BiConsumer<T, List<Annotation>> listConsumer) {
      elements.add(new AnnotationList<>(listConsumer, listProvider, null, null));
      return this;
    }

    public IngesterFormatBuilder<T> annotationList(BiConsumer<T, List<Annotation>> listConsumer,
                                                   int limit) {
      elements.add(new AnnotationList<>(listConsumer, null, limit, null));
      return this;
    }

    public IngesterFormatBuilder<T> annotationMultimap(
        BiConsumer<T, Map<String, List<String>>> multimapConsumer) {
      elements.add(new StringMultiMap<>(multimapConsumer));
      return this;
    }

    public IngesterFormatBuilder<T> textList(BiConsumer<T, List<String>> listConsumer) {
      elements.add(new StringList<>(listConsumer));
      return this;
    }

    public abstract AbstractIngesterFormatter<T> build();
  }

  public static class Text<T extends SpecificRecordBase> implements FormatterElement<T> {
    final List<String> literals;
    final BiConsumer<T, String> textConsumer;
    final boolean isCaseSensitive;

    Text(@Nullable BiConsumer<T, String> textConsumer) {
      this(null, textConsumer, true);
    }

    Text(List<String> literals,
         @Nullable BiConsumer<T, String> textConsumer,
         boolean isCaseSensitive) {
      this.literals = literals;
      this.textConsumer = textConsumer;
      this.isCaseSensitive = isCaseSensitive;
    }

    @Override
    public void consume(StringParser parser, T target) {
      String text = parser.next();
      if (!isAllowedLiteral(text)) throw new ParseException("'" + text +
          "' is not allowed here!");
      if (textConsumer != null) textConsumer.accept(target, text);
    }

    private boolean isAllowedLiteral(String literal) {
      if (literals == null) return true;
      for (String allowedLiteral : literals) {
        if (isCaseSensitive && literal.equals(allowedLiteral)) return true;
        if (!isCaseSensitive && literal.equalsIgnoreCase(allowedLiteral)) return true;
      }
      return false;
    }
  }

  public static class Value<T extends SpecificRecordBase> implements FormatterElement<T> {
    final BiConsumer<T, Double> valueConsumer;

    Value(BiConsumer<T, Double> valueConsumer) {
      this.valueConsumer = valueConsumer;
    }

    @Override
    public void consume(StringParser parser, T target) {
      String token = parser.next();
      if (token == null)
        throw new ParseException("Value is missing");
      try {
        valueConsumer.accept(target, Double.parseDouble(token));
      } catch (NumberFormatException nef) {
        throw new ParseException("Invalid value: " + token);
      }
    }
  }

  public static class Centroids<T extends SpecificRecordBase> implements FormatterElement<T> {
    private static final String WEIGHT = "#";

    @Override
    public void consume(StringParser parser, T target) {
      List<Integer> counts = new ArrayList<>();
      List<Double> bins = new ArrayList<>();

      while (WEIGHT.equals(parser.peek())) {
        parser.next(); // skip the # token
        counts.add(parse(parser.next(), "centroid weight", true).intValue());
        bins.add(parse(parser.next(), "centroid value", false).doubleValue());
      }

      if (counts.size() == 0) throw new ParseException("Empty histogram (no centroids)");

      Histogram histogram = (Histogram) target.get("value");
      histogram.setCounts(counts);
      histogram.setBins(bins);
    }

    private static Number parse(@Nullable String toParse, String name, boolean asInteger) {
      if (toParse == null) {
        throw new ParseException("Unexpected end of line, expected: " + name);
      }
      try {
        return asInteger ? Integer.parseInt(toParse) : Double.parseDouble(toParse);
      } catch (NumberFormatException nef) {
        throw new ParseException("Expected: " + name + ", got: " + toParse);
      }
    }
  }

  public static class Timestamp<T extends SpecificRecordBase> implements FormatterElement<T> {
    private final BiConsumer<T, Long> timestampConsumer;
    private final boolean optional;
    private final boolean raw;

    Timestamp(BiConsumer<T, Long> timestampConsumer, boolean optional, boolean raw) {
      this.timestampConsumer = timestampConsumer;
      this.optional = optional;
      this.raw = raw;
    }

    @Override
    public void consume(StringParser parser, T target) {
      Long timestamp = parseTimestamp(parser, optional, raw);
      if (timestamp != null) timestampConsumer.accept(target, timestamp);
    }
  }

  public static class StringList<T extends SpecificRecordBase> implements FormatterElement<T> {
    private final BiConsumer<T, List<String>> stringListConsumer;

    StringList(BiConsumer<T, List<String>> stringListConsumer) {
      this.stringListConsumer = stringListConsumer;
    }

    @Override
    public void consume(StringParser parser, T target) {
      List<String> list = new ArrayList<>();
      while (parser.hasNext()) {
        list.add(parser.next());
      }
      stringListConsumer.accept(target, list);
    }
  }

  public static class StringMap<T extends SpecificRecordBase> implements FormatterElement<T> {
    private final BiConsumer<T, Map<String, String>> stringMapConsumer;
    private final Function<T, Map<String, String>> stringMapProvider;
    private final Integer limit;
    private final Predicate<String> predicate;

    StringMap(BiConsumer<T, Map<String, String>> stringMapConsumer) {
      this(stringMapConsumer, null, null, null);
    }

    StringMap(BiConsumer<T, Map<String, String>> stringMapConsumer,
              @Nullable Function<T, Map<String, String>> stringMapProvider,
              @Nullable Integer limit,
              @Nullable Predicate<String> predicate) {
      this.stringMapConsumer = stringMapConsumer;
      this.stringMapProvider = stringMapProvider;
      this.limit = limit;
      this.predicate = predicate;
    }

    @Override
    public void consume(StringParser parser, T target) {
      Map<String, String> stringMap = null;
      if (stringMapProvider != null) {
        stringMap = stringMapProvider.apply(target);
      }
      if (stringMap == null) {
        stringMap = Maps.newHashMap();
      }
      int i = 0;
      while (parser.hasNext() && (limit == null || i < limit) &&
          (predicate == null || predicate.test(parser.peek()))) {
        parseKeyValuePair(parser, stringMap::put);
        i++;
      }
      stringMapConsumer.accept(target, stringMap);
    }
  }

  public static class StringMultiMap<T extends SpecificRecordBase> implements FormatterElement<T> {
    private final BiConsumer<T, Map<String, List<String>>> annotationMultimapConsumer;

    StringMultiMap(BiConsumer<T, Map<String, List<String>>> annotationMultimapConsumer) {
      this.annotationMultimapConsumer = annotationMultimapConsumer;
    }

    @Override
    public void consume(StringParser parser, T target) {
      Map<String, List<String>> multimap = new HashMap<>();
      while (parser.hasNext()) {
        parseKeyValuePair(parser, (k, v) -> {
          multimap.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
        });
      }
      annotationMultimapConsumer.accept(target, multimap);
    }
  }

  public static class AnnotationList<T extends SpecificRecordBase> implements FormatterElement<T> {
    private final BiConsumer<T, List<Annotation>> annotationListConsumer;
    private final Function<T, List<Annotation>> annotationListProvider;
    private final Integer limit;
    private final Predicate<String> predicate;

    AnnotationList(BiConsumer<T, List<Annotation>> annotationListConsumer,
                   Predicate<String> predicate) {
      this(annotationListConsumer, null, null, predicate);
    }

    AnnotationList(BiConsumer<T, List<Annotation>> annotationListConsumer,
                   @Nullable Function<T, List<Annotation>> annotationListProvider,
                   @Nullable Integer limit,
                   @Nullable Predicate<String> predicate) {
      this.annotationListConsumer = annotationListConsumer;
      this.annotationListProvider = annotationListProvider;
      this.limit = limit;
      this.predicate = predicate;
    }

    @Override
    public void consume(StringParser parser, T target) {
      List<Annotation> annotations = null;
      if (annotationListProvider != null) {
        annotations = annotationListProvider.apply(target);
      }
      if (annotations == null) {
        annotations = new ArrayList<>();
      }
      List<Annotation> annotationList = annotations;
      int i = 0;
      while (parser.hasNext() && (limit == null || i < limit) &&
          (predicate == null || predicate.test(parser.peek()))) {
        parseKeyValuePair(parser, (k, v) -> annotationList.add(new Annotation(k, v)));
        i++;
      }
      annotationListConsumer.accept(target, annotationList);
    }
  }

  /**
   * Infers timestamp resolution and normalizes it to milliseconds
   * @param timestamp timestamp in seconds, milliseconds, microseconds or nanoseconds
   * @return timestamp in milliseconds
   */
  public static long timestampInMilliseconds(Double timestamp) {
    long timestampLong = timestamp.longValue();
    if (timestampLong < 1_000_000_000_000L) {
      // less than 13 digits: treat it as seconds
      return (long)(1000 * timestamp);
    } else if (timestampLong < 10_000_000_000_000L) {
      // 13 digits: treat as milliseconds
      return timestampLong;
    } else if (timestampLong < 10_000_000_000_000_000L) {
      // 16 digits: treat as microseconds
      return timestampLong / 1000;
    } else {
      // 19 digits: treat as nanoseconds.
      return timestampLong / 1000000;
    }
  }

  private static Long parseTimestamp(StringParser parser, boolean optional, boolean raw) {
    String peek = parser.peek();
    if (peek == null || !Character.isDigit(peek.charAt(0))) {
      if (optional) {
        return null;
      } else {
        throw new ParseException("Expected timestamp, found " +
            (peek == null ? "end of line" : peek));
      }
    }
    try {
      Double timestamp = Double.parseDouble(peek);
      parser.next();
      if (raw) {
        // as-is
        return timestamp.longValue();
      }
      return timestampInMilliseconds(timestamp);
    } catch (NumberFormatException nfe) {
      throw new ParseException("Invalid timestamp value: " + peek);
    }
  }

  private static void parseKeyValuePair(StringParser parser,
                                        BiConsumer<String, String> kvConsumer) {
    String annotationKey = parser.next();
    String op = parser.next();
    if (op == null) {
      throw new ParseException("Tag keys and values must be separated by '=', " +
          "nothing found after '" + annotationKey + "'");
    }
    if (!op.equals("=")) {
      throw new ParseException("Tag keys and values must be separated by '=', found " + op);
    }
    String annotationValue = parser.next();
    if (annotationValue == null) {
      throw new ParseException("Value missing for " + annotationKey);
    }
    kvConsumer.accept(annotationKey, annotationValue);
  }

  /**
   * @param text Text to unquote.
   * @return Extracted value from inside a quoted string.
   */
  @SuppressWarnings("WeakerAccess")  // Has users.
  public static String unquote(String text) {
    if (text.startsWith(DOUBLE_QUOTE_STR)) {
      String quoteless = text.substring(1, text.length() - 1);
      if (containsAny(quoteless, ESCAPED_DOUBLE_QUOTE_STR)) {
        return replace(quoteless, ESCAPED_DOUBLE_QUOTE_STR, DOUBLE_QUOTE_STR);
      }
      return quoteless;
    } else if (text.startsWith(SINGLE_QUOTE_STR)) {
      String quoteless = text.substring(1, text.length() - 1);
      if (containsAny(quoteless, ESCAPED_SINGLE_QUOTE_STR)) {
        return replace(quoteless, ESCAPED_SINGLE_QUOTE_STR, SINGLE_QUOTE_STR);
      }
      return quoteless;
    }
    return text;
  }

  @Nullable
  public static String getHost(@Nullable List<Annotation> annotations,
                               @Nullable List<String> customSourceTags) {
    String source = null;
    String host = null;
    if (annotations != null) {
      Iterator<Annotation> iter = annotations.iterator();
      while (iter.hasNext()) {
        Annotation annotation = iter.next();
        if (annotation.getKey().equals("source")) {
          iter.remove();
          source = annotation.getValue();
        } else if (annotation.getKey().equals("host")) {
          iter.remove();
          host = annotation.getValue();
        } else if (annotation.getKey().equals("tag")) {
          annotation.setKey("_tag");
        }
      }
      if (host != null) {
        if (source == null) {
          source = host;
        } else {
          annotations.add(new Annotation("_host", host));
        }
      }
      if (source == null && customSourceTags != null) {
        // iterate over the set of custom tags, breaking when one is found
        for (String tag : customSourceTags) {
          // nested loops are not pretty but we need to ensure the order of customSourceTags
          for (Annotation annotation : annotations) {
            if (annotation.getKey().equals(tag)) {
              source = annotation.getValue();
              break;
            }
          }
          if (source != null) break;
        }
      }
    }
    return source;
  }

  @Nullable
  public static String getLogMessage(@Nullable List<Annotation> annotations,
                                     @Nullable List<String> customLogMessageTags) {
    String logMessage = null;
    if (annotations != null) {
      Iterator<Annotation> iter = annotations.iterator();
      while (iter.hasNext()) {
        Annotation annotation = iter.next();
        for (String defaultLogMessageKey : DEFAULT_LOG_MESSAGE_KEYS) {
          if (annotation.getKey().equals(defaultLogMessageKey)) {
            iter.remove();
            logMessage = annotation.getValue();
            break;
          }
        }
      }

      if (logMessage == null && customLogMessageTags != null) {
        // iterate over the set of custom message tags, breaking when one is found
        for (String tag : customLogMessageTags) {
          // nested loops are not pretty but we need to ensure the order of customLogMessageTags
          iter = annotations.iterator();
          while (iter.hasNext()) {
            Annotation annotation = iter.next();
            if (annotation.getKey().equals(tag)) {
              logMessage = annotation.getValue();
              iter.remove();
              break;
            }
          }
          if (logMessage != null) break;
        }
      }
    }

    return (logMessage == null)? "" : logMessage;
  }

  @Nullable
  public static Long getLogTimestamp(@Nullable List<Annotation> annotations,
                                       @Nullable List<String> customLogTimestampTags) {
    String timestampStr = null;
    if (annotations != null) {
      Iterator<Annotation> iter = annotations.iterator();
      while (iter.hasNext()) {
        Annotation annotation = iter.next();
        for (String defaultLogTimestampKey : DEFAULT_LOG_TIMESTAMP_KEYS) {
          if (annotation.getKey().equals(defaultLogTimestampKey)) {
            iter.remove();
            timestampStr = annotation.getValue();
            break;
          }
        }
      }

      if (timestampStr == null && customLogTimestampTags != null) {
        // iterate over the set of custom timestamp tags, breaking when one is found
        for (String tag : customLogTimestampTags) {
          // nested loops are not pretty but we need to ensure the order of customLogTimestampTags
          iter = annotations.iterator();
          while (iter.hasNext()) {
            Annotation annotation = iter.next();
            if (annotation.getKey().equals(tag)) {
              timestampStr = annotation.getValue();
              iter.remove();
              break;
            }
          }
          if (timestampStr != null) break;
        }
      }
    }
    if (timestampStr == null) {
      return Clock.now();
    }

    Long timestamp = null;
    // We're only supporting timestamp in epoch format with various resolutions (seconds, milliseconds,
    // microseconds or nanoseconds) as input.  We will normalize to millisecond resolution
    try {
      timestamp = timestampInMilliseconds(Double.parseDouble(timestampStr));
    } catch (NumberFormatException ignore) {
      timestamp = Clock.now();
    }

    return timestamp;
  }

  public T drive(String input, @Nullable Supplier<String> defaultHostNameSupplier,
                 String customerId) {
    return drive(input, defaultHostNameSupplier, customerId, null, null, null, null);
  }

  public abstract T drive(String input, @Nullable Supplier<String> defaultHostNameSupplier,
                          String customerId, @Nullable List<String> customSourceTags, @Nullable List<String> customLogTimestampTags,
                          @Nullable List<String> customLogMessageTags, @Nullable IngesterContext ingesterContext);
}
