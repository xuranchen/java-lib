package com.wavefront.ingester;

import wavefront.report.Annotation;
import wavefront.report.Span;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Builder for Span formatter.
 *
 * @author vasily@wavefront.com
 */
public class SpanIngesterFormatter extends AbstractIngesterFormatter<Span> {

  private SpanIngesterFormatter(List<FormatterElement<Span>> elements) {
    super(elements);
  }

  public static class SpanFormatBuilder extends IngesterFormatBuilder<Span> {
    @Override
    public SpanIngesterFormatter build() {
      return new SpanIngesterFormatter(elements);
    }
  }

  public static IngesterFormatBuilder<Span> newBuilder() {
    return new SpanFormatBuilder();
  }

  @Override
  public Span drive(String input, @Nullable Supplier<String> defaultHostNameSupplier,
                    String customerId, @Nullable List<String> customSourceTags,
                    @Nullable IngesterContext ingesterContext) {
    Span span = new Span();
    span.setCustomer(customerId);
    StringParser parser = new StringParser(input);
    try {
      for (FormatterElement<Span> element : elements) {
        if (ingesterContext != null) {
          element.consume(parser, span, ingesterContext);
        } else {
          element.consume(parser, span);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }
    if (parser.hasNext()) {
      throw new RuntimeException("Unexpected extra input: " + parser.next());
    }
    List<Annotation> annotations = span.getAnnotations();
    if (annotations != null) {
      boolean hasTrueSource = false;
      Iterator<Annotation> iterator = annotations.iterator();
      while (iterator.hasNext()) {
        final Annotation annotation = iterator.next();
        if (customSourceTags != null && !hasTrueSource &&
            customSourceTags.contains(annotation.getKey())) {
          span.setSource(annotation.getValue());
        }
        switch (annotation.getKey()) {
          case "source":
          case "host":
            span.setSource(annotation.getValue());
            iterator.remove();
            hasTrueSource = true;
            break;
          case "spanId":
            span.setSpanId(annotation.getValue());
            iterator.remove();
            break;
          case "traceId":
            span.setTraceId(annotation.getValue());
            iterator.remove();
            break;
          default:
            break;
        }
      }
    }
    if (span.getSource() == null && defaultHostNameSupplier != null) {
      span.setSource(defaultHostNameSupplier.get());
    }
    if (span.getSource() == null) {
      throw new RuntimeException("source can't be null: " + input);
    }
    if (span.getSpanId() == null) {
      throw new RuntimeException("spanId can't be null: " + input);
    }
    if (span.getTraceId() == null) {
      throw new RuntimeException("traceId can't be null: " + input);
    }
    return span;
  }
}
