package com.wavefront.predicates;

import java.util.function.Predicate;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.common.TimeProvider;

import condition.parser.PredicateExpressionLexer;
import condition.parser.PredicateExpressionParser;

/**
 * Utility class for parsing expressions.
 *
 * @author vasily@wavefront.com
 */
public abstract class Predicates {

  private Predicates() {
  }

  /**
   * Parses an expression string into a {@link Predicate<T>}.
   *
   * @param predicateString expression string to parse.
   * @return predicate
   */
  public static <T> Predicate<T> fromPredicateEvalExpression(String predicateString) {
    return new ExpressionPredicate<>(parsePredicateEvalExpression(predicateString));
  }

  @VisibleForTesting
  static PredicateEvalExpression parsePredicateEvalExpression(String predicateString) {
    return parsePredicateEvalExpression(predicateString, System::currentTimeMillis);
  }

  @VisibleForTesting
  static PredicateEvalExpression parsePredicateEvalExpression(String predicateString,
                                                              TimeProvider timeProvider) {
    PredicateExpressionLexer lexer =
        new PredicateExpressionLexer(CharStreams.fromString(predicateString));
    lexer.removeErrorListeners();
    ErrorListener errorListener = new ErrorListener();
    lexer.addErrorListener(errorListener);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    PredicateExpressionParser parser = new PredicateExpressionParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    PredicateExpressionVisitorImpl visitor = new PredicateExpressionVisitorImpl(timeProvider);
    PredicateExpressionParser.ProgramContext context = parser.program();
    PredicateEvalExpression result =
        (PredicateEvalExpression) context.evalExpression().accept(visitor);
    if (errorListener.getErrors().length() == 0) {
      return result;
    } else {
      throw new ExpressionSyntaxException(errorListener.getErrors().toString());
    }
  }
}
