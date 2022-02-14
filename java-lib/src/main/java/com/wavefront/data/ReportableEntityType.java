package com.wavefront.data;

/**
 * Type of objects that Wavefront proxy can send to the server endpoint(s).
 *
 * @author vasily@wavefront.com
 */
public enum ReportableEntityType {
  POINT("points", "pps"),
  DELTA_COUNTER("deltaCounters", "pps"),
  HISTOGRAM("histograms", "dps"),
  SOURCE_TAG("sourceTags", "tags/s"),
  TRACE("spans", "sps"),
  TRACE_SPAN_LOGS("spanLogs", "logs/s"),
  EVENT("events", "eps"),
  LOGS("logs", "logs/s");

  private final String name;
  private final String rateUnit;

  /**
   * @param name     entity name. to be used in metric names as well as log messages.
   * @param rateUnit name of the per second unit, i.e. "rps". to be used in log messages only.
   */
  ReportableEntityType(String name, String rateUnit) {
    this.name = name;
    this.rateUnit = rateUnit;
  }

  @Override
  public String toString() {
    return name;
  }

  public String getRateUnit() {
    return rateUnit;
  }

  public String toCapitalizedString() {
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  public static ReportableEntityType fromString(String name) {
    for (ReportableEntityType type : ReportableEntityType.values()) {
      if (type.toString().equalsIgnoreCase(name)) {
        return type;
      }
    }
    return null;
  }
}
