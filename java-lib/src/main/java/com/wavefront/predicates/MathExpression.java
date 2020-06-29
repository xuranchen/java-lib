package com.wavefront.predicates;

import static com.wavefront.predicates.PredicateEvalExpression.asDouble;
import static com.wavefront.predicates.PredicateEvalExpression.isTrue;

/**
 * A math expression
 *
 * @author vasily@wavefront.com.
 */
public class MathExpression implements PredicateEvalExpression {
  private final PredicateEvalExpression left;
  private final PredicateEvalExpression right;
  private final String op;

  public MathExpression(PredicateEvalExpression left, PredicateEvalExpression right, String op) {
    this.left = left;
    this.right = right;
    this.op = op;
  }

  @Override
  public double getValue(Object entity) {
    switch (op) {
      case "and":
        return asDouble(isTrue(left.getValue(entity)) && isTrue(right.getValue(entity)));
      case "or":
        return asDouble(isTrue(left.getValue(entity)) || isTrue(right.getValue(entity)));
      case "+":
        return left.getValue(entity) + right.getValue(entity);
      case "-":
        return left.getValue(entity) - right.getValue(entity);
      case "*":
        return left.getValue(entity) * right.getValue(entity);
      case "/":
        return left.getValue(entity) / right.getValue(entity);
      case "%":
        return left.getValue(entity) % right.getValue(entity);
      case "=":
        return asDouble(left.getValue(entity) == right.getValue(entity));
      case ">":
        return asDouble(left.getValue(entity) > right.getValue(entity));
      case "<":
        return asDouble(left.getValue(entity) < right.getValue(entity));
      case "<=":
        return asDouble(left.getValue(entity) <= right.getValue(entity));
      case ">=":
        return asDouble(left.getValue(entity) >= right.getValue(entity));
      case "!=":
        return asDouble(left.getValue(entity) != right.getValue(entity));
      case "&":
        return (long) left.getValue(entity) & (long) right.getValue(entity);
      case "|":
        return (long) left.getValue(entity) | (long) right.getValue(entity);
      case "^":
        return (long) left.getValue(entity) ^ (long) right.getValue(entity);
      case ">>":
        return (long) left.getValue(entity) >> (long) right.getValue(entity);
      case ">>>":
        return (long) left.getValue(entity) >>> (long) right.getValue(entity);
      case "<<":
      case "<<<":
        return (long) left.getValue(entity) << (long) right.getValue(entity);
      default:
        throw new IllegalArgumentException("Unknown operator: " + op);
    }
  }
}
