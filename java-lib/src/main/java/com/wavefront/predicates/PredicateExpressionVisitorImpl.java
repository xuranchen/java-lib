package com.wavefront.predicates;

import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.wavefront.common.TimeProvider;

import condition.parser.PredicateExpressionParser;
import condition.parser.PredicateExpressionBaseVisitor;
import wavefront.report.ReportHistogram;
import wavefront.report.ReportMetric;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.wavefront.predicates.PredicateEvalExpression.asDouble;
import static com.wavefront.predicates.PredicateEvalExpression.isTrue;
import static com.wavefront.ingester.AbstractIngesterFormatter.unquote;

/**
 * Expression parser.
 *
 * @author vasily@wavefront.com.
 */
public class PredicateExpressionVisitorImpl extends PredicateExpressionBaseVisitor<BaseExpression> {
  private static final Random RANDOM = new Random();
  private final TimeProvider timeProvider;

  public PredicateExpressionVisitorImpl(TimeProvider timeProvider) {
    this.timeProvider = timeProvider;
  }

  @Override
  public BaseExpression visitEvalExpression(PredicateExpressionParser.EvalExpressionContext ctx) {
    if (ctx == null) {
      throw new ExpressionSyntaxException("Syntax error");
    } else if (ctx.ternary != null) {
      return iff(eval(ctx.evalExpression(0)), eval(ctx.evalExpression(1)),
          eval(ctx.evalExpression(2)));
    } else if (ctx.op != null) {
      return new MathExpression(eval(ctx.evalExpression(0)), eval(ctx.evalExpression(1)),
          ctx.op.getText().toLowerCase().replace(" ", ""));
    } else if (ctx.comparisonOperator() != null) { // = > < <= >= !=
      return new MathExpression(eval(ctx.evalExpression(0)), eval(ctx.evalExpression(1)),
          ctx.comparisonOperator().getText().replace(" ", ""));
    } else if (ctx.not != null) {
      PredicateEvalExpression expression = eval(ctx.evalExpression(0));
      return (PredicateEvalExpression) entity -> asDouble(!isTrue(expression.getValue(entity)));
    } else if (ctx.complement != null) {
      PredicateEvalExpression expression = eval(ctx.evalExpression(0));
      return (PredicateEvalExpression) entity -> ~ (long) expression.getValue(entity);
    } else if (ctx.multiModifier != null) {
      String scope = firstNonNull(ctx.placeholder().Letters(),
          ctx.placeholder().Identifier()).getText();
      StringExpression argument = stringExpression(ctx.stringExpression(0));
      String op = ctx.stringComparisonOp().getText();
      return MultiStringComparisonExpression.of(scope, argument,
          PredicateMatchOp.fromString(ctx.multiModifier.getText()), op);
    } else if (ctx.stringComparisonOp() != null) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      StringExpression right = stringExpression(ctx.stringExpression(1));
      return StringComparisonExpression.of(left, right, ctx.stringComparisonOp().getText());
    } else if (ctx.in != null && ctx.stringExpression().size() > 1) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      List<PredicateEvalExpression> branches = ctx.stringExpression().
          subList(1, ctx.stringExpression().size()).
          stream().
          map(exp -> StringComparisonExpression.of(left, stringExpression(exp), "=")).
          collect(Collectors.toList());
      return (PredicateEvalExpression) entity ->
          asDouble(branches.stream().anyMatch(x -> isTrue(x.getValue(entity))));
    } else if (ctx.stringEvalFunc() != null) {
      StringExpression input = stringExpression(ctx.stringExpression(0));
      if (ctx.stringEvalFunc().strLength() != null) {
        return (PredicateEvalExpression) entity -> input.getString(entity).length();
      } else if (ctx.stringEvalFunc().strHashCode() != null) {
        //noinspection UnstableApiUsage
        return (PredicateEvalExpression) entity ->
            Hashing.murmur3_32().hashString(input.getString(entity), Charsets.UTF_8).asInt();
      } else if (ctx.stringEvalFunc().strIsEmpty() != null) {
        return (PredicateEvalExpression) entity ->
            asDouble(StringUtils.isEmpty(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strIsNotEmpty() != null) {
        return (PredicateEvalExpression) entity ->
            asDouble(StringUtils.isNotEmpty(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strIsBlank() != null) {
        return (PredicateEvalExpression) entity ->
            asDouble(StringUtils.isBlank(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strIsNotBlank() != null) {
        return (PredicateEvalExpression) entity ->
            asDouble(StringUtils.isNotBlank(input.getString(entity)));
      } else if (ctx.stringEvalFunc().strParse() != null) {
        PredicateEvalExpression def = ctx.stringEvalFunc().strParse().evalExpression() == null ?
            x -> 0 : eval(ctx.stringEvalFunc().strParse().evalExpression());
        return (PredicateEvalExpression) entity -> {
          try {
            return Double.parseDouble(input.getString(entity));
          } catch (NumberFormatException e) {
            return def.getValue(entity);
          }
        };
      } else {
        throw new ExpressionSyntaxException("Unknown string eval function");
      }
    } else if (ctx.propertyAccessor() != null) {
      return getPropertyAccessor(ctx.propertyAccessor().getText());
    } else if (ctx.number() != null) {
      return (PredicateEvalExpression) entity -> getNumber(ctx.number());
    } else if (ctx.evalExpression(0) != null) {
      return eval(ctx.evalExpression(0));
    } else {
      return visitChildren(ctx);
    }
  }

  @Override
  public BaseExpression visitStringExpression(
      PredicateExpressionParser.StringExpressionContext ctx) {
    if (ctx.concat != null) {
      StringExpression left = stringExpression(ctx.stringExpression(0));
      StringExpression right = stringExpression(ctx.stringExpression(1));
      return (StringExpression) entity -> left.getString(entity) + right.getString(entity);
    } else if (ctx.stringFunc() != null) {
      StringExpression input = stringExpression(ctx.stringExpression(0));
      if (ctx.stringFunc().strReplace() != null) {
        StringExpression search = stringExpression(ctx.stringFunc().
            strReplace().stringExpression(0));
        StringExpression replacement = stringExpression(ctx.stringFunc().
            strReplace().stringExpression(1));
        return (StringExpression) entity -> input.getString(entity).
            replace(search.getString(entity), replacement.getString(entity));
      } else if (ctx.stringFunc().strReplaceAll() != null) {
        StringExpression regex = stringExpression(ctx.stringFunc().
            strReplaceAll().stringExpression(0));
        StringExpression replacement = stringExpression(ctx.stringFunc().
            strReplaceAll().stringExpression(1));
        return (StringExpression) entity -> input.getString(entity).
            replaceAll(regex.getString(entity), replacement.getString(entity));
      } else if (ctx.stringFunc().strSubstring() != null) {
        PredicateEvalExpression fromExp = eval(ctx.stringFunc().strSubstring().evalExpression(0));
        if (ctx.stringFunc().strSubstring().evalExpression().size() > 1) {
          PredicateEvalExpression toExp = eval(ctx.stringFunc().strSubstring().evalExpression(1));
          return (StringExpression) entity -> input.getString(entity).
              substring((int) fromExp.getValue(entity), (int) toExp.getValue(entity));
        } else {
          return (StringExpression) entity -> input.getString(entity).
              substring((int) fromExp.getValue(entity));
        }
      } else if (ctx.stringFunc().strLeft() != null) {
        PredicateEvalExpression index = eval(ctx.stringFunc().strLeft().evalExpression());
        return (StringExpression) entity -> input.getString(entity).
            substring(0, (int) index.getValue(entity));
      } else if (ctx.stringFunc().strRight() != null) {
        PredicateEvalExpression index = eval(ctx.stringFunc().strRight().evalExpression());
        return (StringExpression) entity -> {
          String str = input.getString(entity);
          return str.substring(str.length() - (int) index.getValue(entity));
        };
      } else if (ctx.stringFunc().strToLowerCase() != null) {
        return (StringExpression) entity -> input.getString(entity).toLowerCase();
      } else if (ctx.stringFunc().strToUpperCase() != null) {
        return (StringExpression) entity -> input.getString(entity).toUpperCase();
      } else {
        throw new ExpressionSyntaxException("Unknown string function");
      }
    } else if (ctx.asString() != null) {
      return visitAsString(ctx.asString());
    } else if (ctx.string() != null) {
      String text = ctx.string().getText();
      return new TemplateStringExpression(ctx.string().Quoted() != null ? unquote(text) : text);
    } else if (ctx.stringExpression(0) != null) {
      return visitStringExpression(ctx.stringExpression(0));
    }
    return visitChildren(ctx);
  }

  @Override
  public BaseExpression visitIff(PredicateExpressionParser.IffContext ctx) {
    if (ctx == null) {
      throw new ExpressionSyntaxException("Syntax error for if()");
    }
    return iff(eval(ctx.evalExpression(0)), eval(ctx.evalExpression(1)),
        eval(ctx.evalExpression(2)));
  }

  @Override
  public BaseExpression visitParse(PredicateExpressionParser.ParseContext ctx) {
    StringExpression strExp = stringExpression(ctx.stringExpression());
    PredicateEvalExpression defaultExp = ctx.evalExpression() == null ?
        x -> 0 : eval(ctx.evalExpression());
    return (PredicateEvalExpression) entity -> {
      try {
        return Double.parseDouble(strExp.getString(entity));
      } catch (NumberFormatException e) {
        return defaultExp.getValue(entity);
      }
    };
  }

  @Override
  public BaseExpression visitTime(PredicateExpressionParser.TimeContext ctx) {
    if (ctx.stringExpression(0) == null) {
      throw new ExpressionSyntaxException("Cannot parse time argument");
    }
    StringExpression timeExp = stringExpression(ctx.stringExpression(0));
    TimeZone tz;
    if (ctx.stringExpression().size() == 1) {
      tz = TimeZone.getTimeZone("UTC");
    } else {
      StringExpression tzExp = stringExpression(ctx.stringExpression(1));
      tz = TimeZone.getTimeZone(tzExp.getString(null));
    }
    // if we can, parse current timestamp against the time argument to fail fast
    String testString = timeExp.getString(null);
    try {
      Util.parseTextualTimeExact(testString, timeProvider.currentTimeMillis(), tz);
    } catch (IllegalArgumentException e) {
      throw new ExpressionSyntaxException("Cannot parse '" + testString + "' as time!");
    }
    return (PredicateEvalExpression) entity ->
        Util.parseTextualTimeExact(timeExp.getString(entity),
            timeProvider.currentTimeMillis(), tz);
  }

  @Override
  public BaseExpression visitEvalLength(PredicateExpressionParser.EvalLengthContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (PredicateEvalExpression) entity -> exp.getString(entity).length();
  }

  @Override
  public BaseExpression visitEvalHashCode(PredicateExpressionParser.EvalHashCodeContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    //noinspection UnstableApiUsage
    return (PredicateEvalExpression) entity ->
        Hashing.murmur3_32().hashString(exp.getString(entity), Charsets.UTF_8).asInt();
  }

  @Override
  public BaseExpression visitEvalIsEmpty(PredicateExpressionParser.EvalIsEmptyContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (PredicateEvalExpression) entity ->
        asDouble(StringUtils.isEmpty(exp.getString(entity)));
  }

  @Override
  public BaseExpression visitEvalIsNotEmpty(PredicateExpressionParser.EvalIsNotEmptyContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (PredicateEvalExpression) entity ->
        asDouble(StringUtils.isNotEmpty(exp.getString(entity)));
  }

  @Override
  public BaseExpression visitEvalIsBlank(PredicateExpressionParser.EvalIsBlankContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (PredicateEvalExpression) entity -> asDouble(StringUtils.isBlank(exp.getString(entity)));
  }

  @Override
  public BaseExpression visitEvalIsNotBlank(PredicateExpressionParser.EvalIsNotBlankContext ctx) {
    StringExpression exp = stringExpression(ctx.stringExpression());
    return (PredicateEvalExpression) entity ->
        asDouble(StringUtils.isNotBlank(exp.getString(entity)));
  }

  @Override
  public BaseExpression visitRandom(PredicateExpressionParser.RandomContext ctx) {
    return (PredicateEvalExpression) entity -> RANDOM.nextDouble();
  }

  @Override
  public BaseExpression visitAsString(PredicateExpressionParser.AsStringContext ctx) {
    PredicateEvalExpression valueExpression = eval(ctx.evalExpression());
    if (ctx.stringExpression() == null) {
      return (StringExpression) entity -> String.valueOf(valueExpression.getValue(entity));
    } else {
      StringExpression format = stringExpression(ctx.stringExpression());
      return (StringExpression) entity ->
          String.format(format.getString(entity), valueExpression.getValue(entity));
    }
  }

  @Override
  public BaseExpression visitStrIff(PredicateExpressionParser.StrIffContext ctx) {
    if (ctx == null) {
      throw new ExpressionSyntaxException("Syntax error for if()");
    }
    PredicateEvalExpression condition = eval(ctx.evalExpression());
    StringExpression thenExpression = stringExpression(ctx.stringExpression(0));
    StringExpression elseExpression = stringExpression(ctx.stringExpression(1));
    return (StringExpression) entity ->
        isTrue(condition.getValue(entity)) ?
            thenExpression.getString(entity) :
            elseExpression.getString(entity);
  }

  private PredicateEvalExpression eval(PredicateExpressionParser.EvalExpressionContext ctx) {
    return (PredicateEvalExpression) visitEvalExpression(ctx);
  }

  private StringExpression stringExpression(PredicateExpressionParser.StringExpressionContext ctx) {
    return (StringExpression) visitStringExpression(ctx);
  }

  private PredicateEvalExpression iff(PredicateEvalExpression cond, PredicateEvalExpression thenExp,
                                      PredicateEvalExpression elseExp) {
    return x -> isTrue(cond.getValue(x)) ? thenExp.getValue(x) : elseExp.getValue(x);
  }

  private PredicateEvalExpression getPropertyAccessor(String property) {
    switch (property) {
      case "value":
        return entity -> {
          if (entity instanceof ReportMetric) {
            return ((ReportMetric) entity).getValue();
          } else if (entity instanceof ReportPoint) {
            return ((ReportPoint) entity).getValue() instanceof Number ?
                ((Number) ((ReportPoint) entity).getValue()).doubleValue() : 0;
          }
          throw new IllegalArgumentException("$value can only be used on a metric, got " +
              entity.getClass().getCanonicalName());
        };
      case "timestamp":
        return entity -> {
          if (entity instanceof ReportMetric) {
            return ((ReportMetric) entity).getTimestamp();
          } else if (entity instanceof ReportHistogram) {
            return ((ReportHistogram) entity).getTimestamp();
          } else if (entity instanceof ReportPoint) {
            return ((ReportPoint) entity).getTimestamp();
          }
          throw new IllegalArgumentException("$timestamp can only be used on a metric or a " +
              "histogram, got " + entity.getClass().getCanonicalName());
        };
      case "startMillis":
        return entity -> {
          if (entity instanceof Span) {
            return ((Span) entity).getStartMillis();
          }
          throw new IllegalArgumentException("$startMillis can only be used on a Span, got " +
              entity.getClass().getCanonicalName());
        };
      case "duration":
        return entity -> {
          if (entity instanceof Span) {
            return ((Span) entity).getDuration();
          }
          throw new IllegalArgumentException("$duration can only be used on a Span, got " +
              entity.getClass().getCanonicalName());
        };
      default:
        throw new ExpressionSyntaxException("Unknown property: " + property);
    }
  }

  private static double getNumber(PredicateExpressionParser.NumberContext numberContext) {
    if (numberContext == null || numberContext.Number() == null) {
      throw new ExpressionSyntaxException("Cannot parse number");
    }
    String text = numberContext.Number().getText();
    double toReturn = text.startsWith("0x") ? Long.decode(text) : Double.parseDouble(text);
    if (numberContext.MinusSign() != null) {
      toReturn = -toReturn;
    }
    if (numberContext.siSuffix() != null) {
      String suffix = numberContext.siSuffix().getText();
      switch (suffix) {
        case "Y":
          toReturn *= 1E24;
          break;
        case "Z":
          toReturn *= 1E21;
          break;
        case "E":
          toReturn *= 1E18;
          break;
        case "P":
          toReturn *= 1E15;
          break;
        case "T":
          toReturn *= 1E12;
          break;
        case "G":
          toReturn *= 1E9;
          break;
        case "M":
          toReturn *= 1E6;
          break;
        case "k":
          toReturn *= 1E3;
          break;
        case "h":
          toReturn *= 1E2;
          break;
        case "da":
          toReturn *= 10;
          break;
        case "d":
          toReturn *= 1E-1;
          break;
        case "c":
          toReturn *= 1E-2;
          break;
        case "m":
          toReturn *= 1E-3;
          break;
        case "µ":
          toReturn *= 1E-6;
          break;
        case "n":
          toReturn *= 1E-9;
          break;
        case "p":
          toReturn *= 1E-12;
          break;
        case "f":
          toReturn *= 1E-15;
          break;
        case "a":
          toReturn *= 1E-18;
          break;
        case "z":
          toReturn *= 1E-21;
          break;
        case "y":
          toReturn *= 1E-24;
          break;
        default:
          throw new ExpressionSyntaxException("Unknown SI Suffix: " + suffix);
      }
    }
    return toReturn;
  }
}
