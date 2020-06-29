package com.wavefront.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Splitter;
import com.google.re2j.Pattern;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RunAutomaton;

/**
 * Predicate for PatternMatching.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class PatternMatch implements Predicate<String> {

  private static final Counter patternsCompiled = Metrics.newCounter(new TaggedMetricName("sldb",
      "patterns.compiled"));
  private static final Counter automatonsCompiled = Metrics.newCounter(new TaggedMetricName("sldb",
      "automatons.compiled"));
  private static final LoadingCache<String, RunAutomaton> automatons = Caffeine.newBuilder().
      expireAfterAccess(30, TimeUnit.MINUTES).
      maximumSize(100_000).
      build(key -> {
        automatonsCompiled.inc();
        return makeAutomaton(key);
      });
  private static final LoadingCache<String, Pattern> patterns = Caffeine.newBuilder().
      expireAfterAccess(30, TimeUnit.MINUTES).
      maximumSize(100_000).
      build(key -> {
        patternsCompiled.inc();
        return Pattern.compile(key);
      });
  private final RunAutomaton pattern;
  private final boolean caseInsensitive;

  private PatternMatch(String pattern, boolean caseInsensitive) {
    this.pattern = automatons.get(caseInsensitive ? pattern.toLowerCase() : pattern);
    this.caseInsensitive = caseInsensitive;
  }

  public static Predicate<String> buildPredicate(String pattern, boolean caseInsensitive) {
    pattern = pattern.trim();
    if (caseInsensitive) pattern = pattern.toLowerCase();
    char[] chars = pattern.toCharArray();
    boolean startsWithWildcard = chars[0] == '*';
    boolean endsWithWildcard = chars[chars.length - 1] == '*';
    // check between the book-ends
    for (int i = 1; i < chars.length - 1; i++) {
      if (chars[i] == '*') {
        // need automaton
        return new PatternMatch(pattern, caseInsensitive);
      }
    }
    if (startsWithWildcard && pattern.length() == 1) return s -> true;
    String finalPattern = pattern;
    return x -> {
      if (x == null) return false;
      if (caseInsensitive) {
        x = x.toLowerCase().trim();
      }
      if (startsWithWildcard && endsWithWildcard) {
        return x.contains(finalPattern.substring(1, finalPattern.length() - 1));
      } else if (startsWithWildcard) {
        return x.endsWith(finalPattern.substring(1));
      } else if (endsWithWildcard) {
        return x.startsWith(finalPattern.substring(0, finalPattern.length() - 1));
      } else {
        return x.equals(finalPattern);
      }
    };
  }

  public static Pattern convert(String pattern) {
    // Basically only support * in the metric name
    Pattern nonStar = patterns.get("([^*]+)");
    Pattern star = patterns.get("[*]");
    String input = nonStar.matcher(pattern).replaceAll("\\\\Q$1\\\\E");
    return patterns.get(star.matcher(input).replaceAll("(.*)"));
  }

  /**
   * Construct a new automaton from a pattern that can only contain *.
   *
   * @param pattern Pattern that can only contain *.
   * @return An automaton that can match the given pattern.
   */
  public static RunAutomaton makeAutomaton(String pattern) {
    pattern = pattern.trim();
    List<String> components = Splitter.on("*").omitEmptyStrings().splitToList(pattern);
    List<Automaton> automata = new ArrayList<>();
    if (pattern.startsWith("*")) {
      automata.add(Automaton.makeAnyString());
    }
    for (int i = 0; i < components.size(); i++) {
      automata.add(Automaton.makeString(components.get(i)));
      if (i < components.size() - 1) {
        automata.add(Automaton.makeAnyString());
      }
    }
    if (pattern.endsWith("*")) {
      automata.add(Automaton.makeAnyString());
    }
    Automaton toReturn = Automaton.concatenate(automata);
    toReturn.minimize();
    return new RunAutomaton(toReturn);
  }

  @Override
  public boolean test(String input) {
    if (caseInsensitive) input = input.toLowerCase();
    return pattern.run(input);
  }
}
