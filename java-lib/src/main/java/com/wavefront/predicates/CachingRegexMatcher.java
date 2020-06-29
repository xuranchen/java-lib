package com.wavefront.predicates;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * A pattern matcher that uses a cache of compiled patterns.
 *
 * @author vasily@wavefront.com.
 */
public class CachingRegexMatcher implements BiFunction<String, String, Boolean> {
  private final LoadingCache<String, Pattern> patternCache;

  public CachingRegexMatcher() {
    this(0);
  }

  public CachingRegexMatcher(int flags) {
     this.patternCache = Caffeine.newBuilder().
        maximumSize(10000).
        expireAfterAccess(1, TimeUnit.MINUTES).
        build(regex -> Pattern.compile(regex, flags));
  }

  @Override
  public Boolean apply(String s, String s2) {
    return patternCache.get(s2).matcher(s).matches();
  }
}
