package com.wavefront.api.agent;

import java.util.UUID;

/**
 * Agent MetricConstants.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class Constants {

  /**
   * Formatted for graphite head
   */
  public static final String PUSH_FORMAT_GRAPHITE = "graphite";
  /**
   * Formatted for graphite head (without customer id in the metric name).
   */
  public static final String PUSH_FORMAT_GRAPHITE_V2 = "graphite_v2";
  public static final String PUSH_FORMAT_WAVEFRONT = "wavefront"; // alias for graphite_v2

  /**
   * Wavefront histogram format
   */
  public static final String PUSH_FORMAT_HISTOGRAM = "histogram";

  /**
   * Wavefront log format
   */
  public static final String PUSH_FORMAT_LOG = "log";

  /**
   * Json Array of logs
   */
  public static final String PUSH_FORMAT_LOGS_JSON_ARR = "logs_json_arr";

  /**
   * Line-delimited JSON objects of logs
   */
  public static final String PUSH_FORMAT_LOGS_JSON_LINE = "logs_json_lines";

  /**
   * Line-delimited format for source tag commands
   */
  public static final String PUSH_FORMAT_SOURCETAGS = "sourceTag";

  /**
   * Line-delimited format for events
   */
  public static final String PUSH_FORMAT_EVENTS = "event";

  /**
   * Wavefront tracing format
   */
  public static final String PUSH_FORMAT_TRACING = "trace";
  public static final String PUSH_FORMAT_TRACING_SPAN_LOGS = "spanLogs";

  /**
   * Work unit id for blocks of graphite-formatted data.
   */
  public static final UUID GRAPHITE_BLOCK_WORK_UNIT =
      UUID.fromString("12b37289-90b2-4b98-963f-75a27110b8da");
}
