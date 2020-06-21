package com.wavefront.ingester;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.StringUtils;
import wavefront.report.Annotation;
import wavefront.report.Histogram;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.wavefront.ingester.IngesterContext.DEFAULT_HISTOGRAM_COMPRESS_LIMIT_RATIO;
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

  private static final IngesterContext DEFAULT_INGESTER_CONTEXT = new IngesterContext.Builder().build();

  protected final List<FormatterElement<T>> elements;

  protected AbstractIngesterFormatter(List<FormatterElement<T>> elements) {
    this.elements = elements;
  }

  protected interface FormatterElement<T> {
    default void consume(StringParser parser, T target) {
      consume(parser, target, DEFAULT_INGESTER_CONTEXT);
    }

    void consume(StringParser parser, T target, IngesterContext ingesterContext);
  }

  /**
   * This class can be used to create a parser for a content that the proxy receives - e.g.,
   * ReportPoint and ReportSourceTag.
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
      elements.add(new AnnotationList<>(listConsumer, x -> !StringUtils.isNumeric(x)));
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
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
      String text = parser.next();
      if (!isAllowedLiteral(text)) throw new RuntimeException("'" + text +
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
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
      String token = parser.next();
      if (token == null)
        throw new RuntimeException("Value is missing");
      try {
        valueConsumer.accept(target, Double.parseDouble(token));
      } catch (NumberFormatException nef) {
        throw new RuntimeException("Invalid value: " + token);
      }
    }
  }

  /**
   * Optimize the means/counts pair if necessary .
   *
   * @param means  centroids means
   * @param counts centroid counts
   */
  public static void optimizeForStorage(@Nullable List<Double> means,
                                        @Nullable List<Integer> counts,
                                        int size, int storageAccuracy) {
    if (means == null || means.isEmpty() || counts == null || counts.isEmpty()) {
      return;
    }

    if /*Too many centroids*/ (size > DEFAULT_HISTOGRAM_COMPRESS_LIMIT_RATIO * storageAccuracy) {
      rewrite(means, counts, size, storageAccuracy);
    }

    if /*Bogus counts*/ (counts.stream().anyMatch(i -> i < 1)) {
      rewrite(means, counts, size, storageAccuracy);
    } else {
      int strictlyIncreasingLength = 1;
      for (; strictlyIncreasingLength < means.size(); ++strictlyIncreasingLength) {
        if (means.get(strictlyIncreasingLength - 1) >= means.get(strictlyIncreasingLength)) {
          break;
        }
      }

      if /*Not ordered*/ (strictlyIncreasingLength != means.size()) {
        rewrite(means, counts, size, storageAccuracy);
      }
    }
  }

  /**
   * Reorganizes a mean/count array pair (such that centroids) are in strictly ascending order.
   *
   * @param means  centroids means
   * @param counts centroid counts
   * @param size  limit for means and counters to rewrite, usually min(means.size(), counts.size())
   */
  private static void rewrite(List<Double> means, List<Integer> counts,
                              int size, int storageAccuracy) {
    TDigest temp = new AVLTreeDigest(storageAccuracy);
    for (int i = 0; i < size; ++i) {
      int count = counts.get(i);
      if (count > 0) {
        temp.add(means.get(i), count);
      }
    }
    temp.compress();

    means.clear();
    counts.clear();
    for (Centroid c : temp.centroids()) {
      means.add(c.mean());
      counts.add(c.count());
    }
  }

  public static class Centroids<T extends SpecificRecordBase> implements FormatterElement<T> {
    private static final String WEIGHT = "#";

    @Override
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
      List<Integer> counts = new ArrayList<>();
      List<Double> bins = new ArrayList<>();

      int size = 0;
      while (WEIGHT.equals(parser.peek())) {
        size++;
        if (size > ingesterContext.getHistogramCentroidsLimit()) {
          throw new TooManyCentroidException();
        }
        parser.next(); // skip the # token
        counts.add(parse(parser.next(), "centroid weight", true).intValue());
        bins.add(parse(parser.next(), "centroid value", false).doubleValue());
      }

      if (size == 0) throw new RuntimeException("Empty histogram (no centroids)");

      optimizeForStorage(bins, counts, size, ingesterContext.getTargetHistogramAccuracy());

      Histogram histogram = (Histogram) target.get("value");
      histogram.setCounts(counts);
      histogram.setBins(bins);
    }

    private static Number parse(@Nullable String toParse, String name, boolean asInteger) {
      if (toParse == null) {
        throw new RuntimeException("Unexpected end of line, expected: " + name);
      }
      try {
        return asInteger ? Integer.parseInt(toParse) : Double.parseDouble(toParse);
      } catch (NumberFormatException nef) {
        throw new RuntimeException("Expected: " + name + ", got: " + toParse);
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
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
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
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
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
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
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
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
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
    private final Predicate<String> predicate;

    AnnotationList(BiConsumer<T, List<Annotation>> annotationListConsumer,
                   Predicate<String> predicate) {
      this.annotationListConsumer = annotationListConsumer;
      this.predicate = predicate;
    }

    @Override
    public void consume(StringParser parser, T target, IngesterContext ingesterContext) {
      List<Annotation> annotationList = new ArrayList<>();
      while (parser.hasNext() && predicate.test(parser.peek())) {
        parseKeyValuePair(parser, (k, v) -> annotationList.add(new Annotation(k, v)));
      }
      annotationListConsumer.accept(target, annotationList);
    }
  }

  private static Long parseTimestamp(StringParser parser, boolean optional, boolean raw) {
    String peek = parser.peek();
    if (peek == null || !Character.isDigit(peek.charAt(0))) {
      if (optional) {
        return null;
      } else {
        throw new RuntimeException("Expected timestamp, found " +
            (peek == null ? "end of line" : peek));
      }
    }
    try {
      Double timestamp = Double.parseDouble(peek);
      long timestampLong = timestamp.longValue();
      parser.next();
      if (raw) {
        // as-is
        return timestampLong;
      }
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
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("Invalid timestamp value: " + peek);
    }
  }

  private static void parseKeyValuePair(StringParser parser,
                                        BiConsumer<String, String> kvConsumer) {
    String annotationKey = parser.next();
    String op = parser.next();
    if (op == null) {
      throw new RuntimeException("Tag keys and values must be separated by '=', " +
          "nothing found after '" + annotationKey + "'");
    }
    if (!op.equals("=")) {
      throw new RuntimeException("Tag keys and values must be separated by '=', found " + op);
    }
    String annotationValue = parser.next();
    if (annotationValue == null) {
      throw new RuntimeException("Value missing for " + annotationKey);
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

  public T drive(String input, @Nullable Supplier<String> defaultHostNameSupplier,
                 String customerId) {
    return drive(input, defaultHostNameSupplier, customerId, null, null);
  }

  public T drive(String input, @Nullable Supplier<String> defaultHostNameSupplier,
                 String customerId, IngesterContext ingesterContext) {
    return drive(input, defaultHostNameSupplier, customerId, null, ingesterContext);
  }

  public abstract T drive(String input, @Nullable Supplier<String> defaultHostNameSupplier,
                          String customerId, @Nullable List<String> customSourceTags,
                          @Nullable IngesterContext ingesterContext);
}
