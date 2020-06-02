package com.wavefront.ingester;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Convert {@link SpanLogs} to its string representation in a canonical format.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class SpanLogsSerializer implements Function<SpanLogs, String> {
  private static final Logger logger =
      Logger.getLogger(SpanLogsSerializer.class.getCanonicalName());

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  static {
    JSON_PARSER.addMixIn(SpanLogs.class, IgnoreSchemaProperty.class);
    JSON_PARSER.addMixIn(SpanLog.class, IgnoreSchemaProperty.class);
  }

  @Override
  public String apply(SpanLogs spanLogs) {
    return spanLogsToString(spanLogs);
  }

  @VisibleForTesting
  static String spanLogsToString(SpanLogs spanLogs) {
    try {
      return JSON_PARSER.writeValueAsString(spanLogs);
    } catch (JsonProcessingException e) {
      logger.warning("Serialization error!");
      return null;
    }
  }

  abstract static class IgnoreSchemaProperty {
    @JsonIgnore
    abstract void getSchema();
  }
}
