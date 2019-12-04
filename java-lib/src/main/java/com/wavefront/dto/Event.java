package com.wavefront.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import wavefront.report.ReportEvent;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Wrapper for the Event class.
 *
 * @author vasily@wavefront.com
 */
public class Event implements Serializable {
  private ReportEvent event;

  public Event(ReportEvent event) {
    this.event = event;
  }

  @JsonProperty
  public String getName() {
    return event.getName();
  }

  @JsonProperty
  public long getStartTime() {
    return event.getStartTime();
  }

  @JsonProperty
  public Long getEndTime() {
    return event.getEndTime();
  }

  @JsonProperty
  public Map<String, String> getAnnotations() {
    return event.getAnnotations();
  }

  @JsonProperty
  public Map<String, List<String>> getDimensions() {
    return event.getDimensions();
  }

  @JsonProperty
  public List<String> getHosts() {
    return event.getHosts();
  }

  @JsonProperty
  public List<String> getTags() {
    return event.getTags();
  }
}
