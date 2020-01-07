package com.wavefront.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import wavefront.report.ReportSourceTag;

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
  private String sourceTagLiteral;

  @JsonProperty
  private String action;

  @JsonProperty
  private String source;

  @JsonProperty
  private String description;

  @JsonProperty
  private List<String> annotations;

  @SuppressWarnings("unused")
  private SourceTag() {
  }

  public SourceTag(ReportSourceTag sourceTag) {
    this.sourceTagLiteral = sourceTag.getSourceTagLiteral();
    this.action = sourceTag.getAction();
    this.source = sourceTag.getSource();
    this.description = sourceTag.getDescription();
    this.annotations = new ArrayList<>(sourceTag.getAnnotations());
  }

  public String getSourceTagLiteral() {
    return sourceTagLiteral;
  }

  public String getAction() {
    return action;
  }

  public String getSource() {
    return source;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getAnnotations() {
    return annotations;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + (sourceTagLiteral == null ? 0 : sourceTagLiteral.hashCode());
    result = result * 31 + (action == null ? 0 : action.hashCode());
    result = result * 31 + (source == null ? 0 : source.hashCode());
    result = result * 31 + (description == null ? 0 : description.hashCode());
    result = result * 31 + (annotations == null ? 0 : annotations.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    SourceTag other = (SourceTag) obj;
    if (!Objects.equals(this.sourceTagLiteral, other.sourceTagLiteral)) return false;
    if (!Objects.equals(this.action, other.action)) return false;
    if (!Objects.equals(this.source, other.source)) return false;
    if (!Objects.equals(this.description, other.description)) return false;
    if (!Objects.equals(this.annotations, other.annotations)) return false;
    return true;
  }
}
