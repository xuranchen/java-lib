package com.wavefront.predicates;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

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
    } else if (entity instanceof ReportPoint) {
      return Util.expandPlaceholders(template, (ReportPoint) entity);
    } else if (entity instanceof Span) {
      return Util.expandPlaceholders(template, (Span) entity);
    } else {
      throw new IllegalArgumentException(entity.getClass().getCanonicalName() +
          " is not supported!");
    }
  }
}
