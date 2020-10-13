package com.wavefront.common.logger;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;
import com.wavefront.common.Pair;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A logger that suppresses identical messages for a specified period of time, and prints the
 * number of messages skipped during next period.
 *
 * @author vasily@wavefront.com
 */
@SuppressWarnings("UnstableApiUsage")
public class MessageCountingLogger extends DelegatingLogger {
  private final LoadingCache<String, Pair<RateLimiter, AtomicLong>> rateLimiterCache;

  /**
   * @param delegate     Delegate logger.
   * @param maximumSize  max number of unique messages that can exist in the cache
   * @param rateLimit    rate limit (per second per each unique message)
   */
  public MessageCountingLogger(Logger delegate,
                               long maximumSize,
                               double rateLimit) {
    super(delegate);
    this.rateLimiterCache = Caffeine.newBuilder().
        expireAfterAccess((long)(2 / rateLimit), TimeUnit.SECONDS).
        maximumSize(maximumSize).
        build(x -> Pair.of(RateLimiter.create(rateLimit), new AtomicLong()));
  }

  @Override
  public void log(Level level, String message) {
    Pair<RateLimiter, AtomicLong> limiter = Objects.requireNonNull(rateLimiterCache.get(message));
    if (limiter._1.tryAcquire()) {
      long skipped = limiter._2.getAndSet(0);
      if (skipped > 0) {
        log(new LogRecord(level, message + " [" + skipped + " duplicate messages skipped]"));
      } else {
        log(new LogRecord(level, message));
      }
    } else {
      limiter._2.incrementAndGet();
    }
  }
}
