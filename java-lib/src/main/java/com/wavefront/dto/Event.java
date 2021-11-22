package com.wavefront.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.ingester.AbstractIngesterFormatter;
import com.wavefront.ingester.EventDecoder;
import wavefront.report.ReportEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.wavefront.common.SerializerUtils.appendQuoted;
import static com.wavefront.common.SerializerUtils.appendTagMap;
import static com.wavefront.common.SerializerUtils.appendTags;

/**
 * Serializing wrapper for the Event class.
 *
 * @author vasily@wavefront.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event implements Serializable {
  @JsonProperty
  private String name;
  @JsonProperty
  private long startTime;
  @JsonProperty
  private Long endTime;
  @JsonProperty
  private Map<String, String> annotations;
  @JsonProperty
  private Map<String, List<String>> dimensions;
  @JsonProperty
  private List<String> hosts;
  @JsonProperty
  private List<String> tags;

  @SuppressWarnings("unused")
  private Event() {
  }

  public Event(ReportEvent event) {
    this.name = event.getName();
    this.startTime = event.getStartTime();
    this.endTime = event.getEndTime();
    this.annotations = new HashMap<>(event.getAnnotations());
    this.dimensions = event.getDimensions() == null ? null :
        event.getDimensions().entrySet().stream().
            collect(Collectors.toMap(Map.Entry::getKey, v -> new ArrayList<>(v.getValue())));
    this.hosts = new ArrayList<>(event.getHosts());
    this.tags = event.getTags() == null ? null : new ArrayList<>(event.getTags());
  }

  public String getName() {
    return name;
  }

  public long getStartTime() {
    return startTime;
  }

  @JsonProperty
  public Long getEndTime() {
    return endTime;
  }

  @JsonProperty
  public Map<String, String> getAnnotations() {
    return annotations;
  }

  @JsonProperty
  public Map<String, List<String>> getDimensions() {
    return dimensions;
  }

  @JsonProperty
  public List<String> getHosts() {
    return hosts;
  }

  @JsonProperty
  public List<String> getTags() {
    return tags;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + (name == null ? 0 : name.hashCode());
    result = result * 31 + (int) (startTime ^ (startTime >>> 32));
    result = result * 31 + (endTime == null ? 0 : (int) (endTime ^ (endTime >>> 32)));
    result = result * 31 + annotations.hashCode();
    result = result * 31 + (dimensions == null ? 0 : dimensions.hashCode());
    result = result * 31 + hosts.hashCode();
    result = result * 31 + (tags == null ? 0 : tags.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    Event other = (Event) obj;
    if (!Objects.equals(name, other.name)) return false;
    if (startTime != other.startTime) return false;
    if (!Objects.equals(endTime, other.endTime)) return false;
    if (!annotations.equals(other.annotations)) return false;
    if (!Objects.equals(dimensions, other.dimensions)) return false;
    if (!hosts.equals(other.hosts)) return false;
    if (!Objects.equals(tags, other.tags)) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(AbstractIngesterFormatter.EVENT_LITERAL).append(' ');
    sb.append(this.getStartTime()).append(' ');
    if (this.getEndTime() != null) {
      sb.append(this.getEndTime());
    }
    sb.append(' ');
    appendQuoted(sb, this.getName());
    appendTags(sb, "host", this.getHosts());
    if (this.getAnnotations() != null) {
      appendTagMap(sb, this.getAnnotations());
    }
    if (this.getDimensions() != null) {
      for (Map.Entry<String, List<String>> entry : this.getDimensions().entrySet()) {
        appendTags(sb, entry.getKey(), entry.getValue());
      }
    }
    if (this.getTags() != null) {
      appendTags(sb, "tag", this.getTags());
    }
    return sb.toString();
  }
}
