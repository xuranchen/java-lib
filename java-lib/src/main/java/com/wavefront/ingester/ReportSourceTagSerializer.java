package com.wavefront.ingester;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import wavefront.report.ReportSourceTag;

import java.util.function.Function;

import static com.wavefront.ingester.AbstractIngesterFormatter.SOURCE_DESCRIPTION_LITERAL;
import static com.wavefront.ingester.AbstractIngesterFormatter.SOURCE_TAG_LITERAL;

/**
 * Convert a {@link ReportSourceTag} to its string representation.
 *
 * @author vasily@wavefront.com
 */
public class ReportSourceTagSerializer implements Function<ReportSourceTag, String> {
  @Override
  public String apply(ReportSourceTag input) {
    return sourceTagToString(input);
  }

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, "\"", "\\\"");
  }

  @VisibleForTesting
  static String sourceTagToString(ReportSourceTag sourceTag) {
    StringBuilder sb = new StringBuilder();
    switch (sourceTag.getOperation()) {
      case SOURCE_TAG:
        sb.append(SOURCE_TAG_LITERAL);
        break;
      case SOURCE_DESCRIPTION:
        sb.append(SOURCE_DESCRIPTION_LITERAL);
        break;
      default:
        throw new IllegalArgumentException("Unknown operation: " + sourceTag.getOperation());
    }
    sb.append(" action=");
    switch (sourceTag.getAction()) {
      case SAVE:
        sb.append("save");
        break;
      case ADD:
        sb.append("add");
        break;
      case DELETE:
        sb.append("delete");
        break;
      default:
        sb.append("<unknown>");
    }
    sb.append(" source=\"");
    sb.append(escapeQuotes(sourceTag.getSource()));
    sb.append("\" ");
    sourceTag.getAnnotations().forEach(x -> sb.append("\"").append(escapeQuotes(x)).append("\""));
    return sb.toString();
  }
}
