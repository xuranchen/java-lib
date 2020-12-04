package com.wavefront.api.agent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Represents span sampling policy.
 *
 * @author Shipeng Xie (xshipeng@vmware.com)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SpanSamplingPolicy implements Serializable {

  private String policyId;
  private String expression;
  private int samplingPercent;

  @SuppressWarnings("unused")
  private SpanSamplingPolicy() {
  }

  /**
   * Construct span sampling policy.
   *
   * @param policyId        Id of the policy
   * @param expression      String expression of the policy
   * @param samplingPercent Sampling percent of the policy, should be an integer in [0,100]
   */
  public SpanSamplingPolicy(@Nonnull String policyId, @Nonnull String expression,
                            int samplingPercent) {
    this.policyId = policyId;
    this.expression = expression;
    this.samplingPercent = samplingPercent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SpanSamplingPolicy policy = (SpanSamplingPolicy) o;
    return samplingPercent == policy.samplingPercent &&
        Objects.equals(policyId, policy.policyId) &&
        Objects.equals(expression, policy.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(policyId, expression, samplingPercent);
  }

  @Nonnull
  public String getPolicyId() {
    return policyId;
  }

  public void setPolicyId(@Nonnull String policyId) {
    this.policyId = policyId;
  }

  @Nonnull
  public String getExpression() {
    return expression;
  }

  public void setExpression(@Nonnull String expression) {
    this.expression = expression;
  }

  public int getSamplingPercent() {
    return samplingPercent;
  }

  public void setSamplingPercent(int samplingPercent) {
    this.samplingPercent = samplingPercent;
  }
}
