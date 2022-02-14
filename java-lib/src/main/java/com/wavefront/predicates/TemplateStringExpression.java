package com.wavefront.predicates;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportHistogram;
import wavefront.report.ReportMetric;
import wavefront.report.ReportPoint;
import wavefront.report.Span;
import wavefront.report.ReportLog;

/**
 * A string template rendered. Substitutes {{...}} placeholders with corresponding
 * components; string literals are returned as is.
 *
 * @author vasily@wavefront.com
 */
public class TemplateStringExpression implements StringExpression {
  private final String template;

  public TemplateStringExpression(String template) {
    this.template = template;
  }

  @Nonnull
  @Override
  public String getString(@Nullable Object entity) {
    if (entity == null) {
      return template;
    } else if (entity instanceof ReportMetric) {
      return Util.expandPlaceholders(template, (ReportMetric) entity);
    } else if (entity instanceof ReportHistogram) {
      return Util.expandPlaceholders(template, (ReportHistogram) entity);
    } else if (entity instanceof ReportPoint) {
      return Util.expandPlaceholders(template, (ReportPoint) entity);
    } else if (entity instanceof Span) {
      return Util.expandPlaceholders(template, (Span) entity);
    } else if (entity instanceof ReportLog) {
      return Util.expandPlaceholders(template, (ReportLog) entity);
    } else {
      throw new IllegalArgumentException(entity.getClass().getCanonicalName() +
          " is not supported!");
    }
  }
}
