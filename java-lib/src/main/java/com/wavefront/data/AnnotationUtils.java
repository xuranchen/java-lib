package com.wavefront.data;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import wavefront.report.Annotation;

/**
 * Utility methods for working with {@code List&lt;Annotation&gt;}.
 *
 * @author vasily@wavefront.com
 */
public abstract class AnnotationUtils {
  private AnnotationUtils() {
  }

  public static SetMultimap<String, String> toMultimap(@Nullable Collection<Annotation> annotations) {
    if (annotations == null) return ImmutableSetMultimap.of();
    final ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
    for (Annotation annotation : annotations) {
      builder.put(annotation.getKey(), annotation.getValue());
    }
    return builder.build();
  }

  public static List<Annotation> toAnnotationList(@Nullable Multimap<String, String> tags) {
    if (tags == null || tags.isEmpty()) return ImmutableList.of();
    return tags.entries().stream().map(t -> new Annotation(t.getKey(), t.getValue())).
        collect(Collectors.toList());
  }

  public static List<Annotation> toAnnotationList(@Nullable Map<String, String> tags) {
    if (tags == null || tags.isEmpty()) return ImmutableList.of();
    return tags.entrySet().stream().map(t -> new Annotation(t.getKey(), t.getValue())).
        collect(Collectors.toList());
  }

  @Nullable
  public static String getValue(List<Annotation> annotations, String key) {
    return annotations.stream().filter(x -> x.getKey().equals(key)).findFirst().
        map(Annotation::getValue).orElse(null);
  }
}
