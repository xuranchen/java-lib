package com.wavefront.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceTagAction;
import wavefront.report.SourceOperationType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Serializing wrapper for the SourceTag class.
 *
 * @author vasily@wavefront.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceTag implements Serializable {
  @JsonProperty
  private SourceOperationType operation;

  @JsonProperty
  private SourceTagAction action;

  @JsonProperty
  private String source;

  @JsonProperty
  private List<String> annotations;

  @SuppressWarnings("unused")
  private SourceTag() {
  }

  public SourceTag(ReportSourceTag sourceTag) {
    this.operation = sourceTag.getOperation();
    this.action = sourceTag.getAction();
    this.source = sourceTag.getSource();
    this.annotations = new ArrayList<>(sourceTag.getAnnotations());
  }

  public SourceOperationType getOperation() {
    return operation;
  }

  public SourceTagAction getAction() {
    return action;
  }

  public String getSource() {
    return source;
  }

  public List<String> getAnnotations() {
    return annotations;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + (operation == null ? 0 : operation.hashCode());
    result = result * 31 + (action == null ? 0 : action.hashCode());
    result = result * 31 + (source == null ? 0 : source.hashCode());
    result = result * 31 + (annotations == null ? 0 : annotations.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    SourceTag other = (SourceTag) obj;
    if (!Objects.equals(this.operation, other.operation)) return false;
    if (!Objects.equals(this.action, other.action)) return false;
    if (!Objects.equals(this.source, other.source)) return false;
    if (!Objects.equals(this.annotations, other.annotations)) return false;
    return true;
  }
}
