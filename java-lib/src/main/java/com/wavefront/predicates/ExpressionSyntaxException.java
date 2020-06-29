package com.wavefront.predicates;

/**
 * Catch-all exception for parse-time syntax errors.
 *
 * @author vasily@wavefront.com
 */
public class ExpressionSyntaxException extends RuntimeException {
  public ExpressionSyntaxException(String message) {
    super(message);
  }
}
