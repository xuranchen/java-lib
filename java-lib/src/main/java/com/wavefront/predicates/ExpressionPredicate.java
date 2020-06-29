package com.wavefront.predicates;

import java.util.function.Predicate;

import static com.wavefront.predicates.PredicateEvalExpression.isTrue;

/**
 * {@link PredicateEvalExpression} to {@link Predicate<T>} adapter.
 *
 * @author vasily@wavefront.com.
 */
public class ExpressionPredicate<T> implements Predicate<T> {
  private final PredicateEvalExpression wrapped;

  public ExpressionPredicate(PredicateEvalExpression wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public boolean test(T t) {
    return isTrue(wrapped.getValue(t));
  }
}
