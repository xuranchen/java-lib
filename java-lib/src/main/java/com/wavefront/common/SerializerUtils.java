package com.wavefront.common;

import org.apache.commons.lang.StringUtils;
import wavefront.report.Annotation;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Common utility methods used by serializers.
 *
 * @author vasily@wavefront.com
 */
public abstract class SerializerUtils {
  private static final String DOUBLE_QUOTE = "\"";
  private static final String ESCAPED_DOUBLE_QUOTE = "\\\"";

  public static StringBuilder appendQuoted(StringBuilder sb, String raw) {
    return sb.append(DOUBLE_QUOTE).append(escapeQuotes(raw)).append(DOUBLE_QUOTE);
  }

  /**
   * Append a {@code Map<String, String>} to a string builder as double-quoted key-value pairs.
   *
   * @param sb   A {@link StringBuilder instance}
   * @param tags Aap of tags.
   * @return StringBuilder instance
   */
  public static StringBuilder appendTagMap(StringBuilder sb, @Nullable Map<String, String> tags) {
    if (tags != null) {
      for (Map.Entry<String, String> entry : tags.entrySet()) {
        sb.append(' ');
        appendQuoted(sb, entry.getKey()).append('=');
        appendQuoted(sb, entry.getValue());
      }
    }
    return sb;
  }

  /**
   * Append a {@code List<Annotation>} to a string builder as double-quoted key-value pairs.
   *
   * @param sb          A {@link StringBuilder instance}
   * @param annotations List of annotations.
   * @return StringBuilder instance
   */
  public static StringBuilder appendAnnotations(StringBuilder sb,
                                                @Nullable List<Annotation> annotations) {
    if (annotations != null) {
      for (Annotation annotation : annotations) {
        sb.append(' ');
        appendQuoted(sb, annotation.getKey()).append('=');
        appendQuoted(sb, annotation.getValue());
      }
    }
    return sb;
  }

  /**
   * Append a {@code List<String>} to a string builder as double-quoted key-value pairs
   * with a fixed key.
   *
   * @param sb        A {@link StringBuilder instance}
   * @param tagKey    Key to use
   * @param tagValues List of tags to append.
   * @return StringBuilder instance
   */
  public static StringBuilder appendTags(StringBuilder sb, String tagKey,
                                         @Nullable List<String> tagValues) {
    if (tagValues != null) {
      for (String tag : tagValues) {
        sb.append(' ');
        appendQuoted(sb, tagKey).append('=');
        appendQuoted(sb, tag);
      }
    }
    return sb;
  }

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, DOUBLE_QUOTE, ESCAPED_DOUBLE_QUOTE);
  }
}
