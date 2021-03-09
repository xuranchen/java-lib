package com.wavefront.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.ingester.AbstractIngesterFormatter;
import wavefront.report.ReportEvent;
import wavefront.report.Annotation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.wavefront.common.SerializerUtils.appendQuoted;
import static com.wavefront.common.SerializerUtils.appendAnnotations;
import static com.wavefront.common.SerializerUtils.appendTags;

/**
 * Serializing wrapper for the Event class.
 *
 * @author vasily@wavefront.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event implements Serializable {
  @JsonProperty
  private String group;
  @JsonProperty
  private String eventId;
  @JsonProperty
  private long startMillis;
  @JsonProperty
  private Long endMillis;
  @JsonProperty
  private String customer;
  @JsonProperty
  private List<String> hosts;
  @JsonProperty
  private List<Annotation> annotations;
  @JsonProperty
  private String details;

  @SuppressWarnings("unused")
  private Event() {
  }

  public Event(ReportEvent event) {
    this.group = event.getGroup();
    this.eventId = event.getEventId();
    this.startMillis = event.getStartMillis();
    this.endMillis = event.getEndMillis();
    this.annotations = event.getAnnotations();
    this.customer = event.getCustomer();
    this.hosts = new ArrayList<>(event.getHosts());
    this.details = event.getDetails();
  }

  public String getGroup() {
    return group;
  }

  public String getEventId() { return eventId; }

  public long getStartMillis() {
    return startMillis;
  }

  @JsonProperty
  public Long getEndMillis() {
    return endMillis;
  }

  @JsonProperty
  public List<Annotation> getAnnotations() {
    return annotations;
  }

  @JsonProperty
  public String getCustomer() { return customer; }

  @JsonProperty
  public List<String> getHosts() {
    return hosts;
  }

  @JsonProperty
  public String getDetails() {
    return details;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + (group == null ? 0 : group.hashCode());
    result = result * 31 + (eventId == null ? 0 : eventId.hashCode());
    result = result * 31 + (int) (startMillis ^ (startMillis >>> 32));
    result = result * 31 + (endMillis == null ? 0 : (int) (endMillis ^ (endMillis >>> 32)));
    result = result * 31 + annotations.hashCode();
    result = result * 31 + customer.hashCode();
    result = result * 31 + hosts.hashCode();
    result = result * 31 + (details == null ? 0 : details.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    Event other = (Event) obj;
    if (!Objects.equals(group, other.group)) return false;
    if (!Objects.equals(eventId, other.eventId)) return false;
    if (startMillis != other.startMillis) return false;
    if (!Objects.equals(endMillis, other.endMillis)) return false;
    if (!annotations.equals(other.annotations)) return false;
    if (!customer.equals(other.customer)) return false;
    if (!hosts.equals(other.hosts)) return false;
    if (!details.equals(other.details)) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(AbstractIngesterFormatter.EVENT_LITERAL).append(' ');
    sb.append(this.getStartMillis()).append(' ');
    if (this.getEndMillis() != null) {
      sb.append(this.getEndMillis());
    }
    sb.append(' ');
    appendQuoted(sb, this.getGroup());
    sb.append(' ');
    appendQuoted(sb, this.getEventId());
    sb.append(' ');
    appendQuoted(sb, this.getCustomer());
    appendTags(sb, "host", this.getHosts());
    if (this.getAnnotations() != null) {
      appendAnnotations(sb, this.getAnnotations());
    }
    if (this.getDetails() != null) {
      appendQuoted(sb, this.getDetails());
    }
    return sb.toString();
  }
}
