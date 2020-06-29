package com.wavefront.predicates;

import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.wavefront.common.PatternMatch;

import static com.wavefront.predicates.PredicateEvalExpression.asDouble;

/**
 * Adapter that converts two {@link StringExpression} to {@link PredicateEvalExpression}.
 *
 * @author vasily@wavefront.com.
 */
public class StringComparisonExpression implements PredicateEvalExpression {

  private final StringExpression left;
  private final StringExpression right;
  private final BiFunction<String, String, Boolean> func;

  private StringComparisonExpression(StringExpression left,
                                     StringExpression right,
                                     BiFunction<String, String, Boolean> func) {
    this.left = left;
    this.right = right;
    this.func = func;
  }

  @Override
  public double getValue(Object entity) {
    return asDouble(func.apply(left.getString(entity), right.getString(entity)));
  }

  public static PredicateEvalExpression of(StringExpression left, StringExpression right,
                                           String op) {
    switch (op) {
      case "=":
      case "equals":
        return new StringComparisonExpression(left, right, String::equals);
      case "startsWith":
        return new StringComparisonExpression(left, right, String::startsWith);
      case "endsWith":
        return new StringComparisonExpression(left, right, String::endsWith);
      case "contains":
        return new StringComparisonExpression(left, right, String::contains);
      case "matches":
        Predicate<String> patternMatch = PatternMatch.buildPredicate(right.getString(null), false);
        return entity -> asDouble(patternMatch.test(left.getString(entity)));
      case "regexMatch":
        return new StringComparisonExpression(left, right, new CachingRegexMatcher());
      case "equalsIgnoreCase":
        return new StringComparisonExpression(left, right, String::equalsIgnoreCase);
      case "startsWithIgnoreCase":
        return new StringComparisonExpression(left, right, StringUtils::startsWithIgnoreCase);
      case "endsWithIgnoreCase":
        return new StringComparisonExpression(left, right, StringUtils::endsWithIgnoreCase);
      case "containsIgnoreCase":
        return new StringComparisonExpression(left, right, StringUtils::containsIgnoreCase);
      case "matchesIgnoreCase":
        Predicate<String> patternMatchCI = PatternMatch.buildPredicate(right.getString(null), true);
        return entity -> asDouble(patternMatchCI.test(left.getString(entity)));
      case "regexMatchIgnoreCase":
        return new StringComparisonExpression(left, right,
            new CachingRegexMatcher(Pattern.CASE_INSENSITIVE));
      default:
        throw new IllegalArgumentException(op + " is not handled");
    }
  }
}
