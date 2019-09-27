package com.google.common.util.concurrent;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * A rate limiter that allows to "return" unused permits back to the pool to handle retries
 * gracefully and allow precise control over outgoing "successful" rate, plus allows accumulating
 * "credits" for unused permits over a time window other than 1 second.
 *
 * @author vasily@wavefront.com
 */
public interface RecyclableRateLimiter {

  /**
   * Returns the currently configured rate per second at which new permits become available.
   *
   * @return number of permits per second
   */
  double getRate();

  /**
   * Updates the rate for this {@code RateLimiter}.
   *
   * @param rate new rate per second
   */
  void setRate(double rate);

  /**
   * Acquires the requested number of permits, waiting until enough permits are available.
   *
   * @param permits number of permits to request
   * @return wait time in milliseconds
   */
  @CanIgnoreReturnValue
  double acquire(int permits);

  /**
   * Acquires a single permit, waiting until enough permits are available.
   */
  @CanIgnoreReturnValue
  default double acquire() {
    return acquire(1);
  }

  /**
   * Acquires the requested number of permits only if can be acquired without wait.
   *
   * @param permits number of permits to request
   * @return true if permits were acquired
   */
  boolean tryAcquire(int permits);

  /**
   * Acquires a single permit only if can be acquired without wait.
   *
   * @return true if permits were acquired
   */
  default boolean tryAcquire() {
    return tryAcquire(1);
  }

  /**
   * Return the specified number of permits back to the pool.
   *
   * @param permits number of permits to return
   */
  void recyclePermits(int permits);

  /**
   * Checks whether there's enough permits accumulated to cover the number of requested permits.
   *
   * @param permits permits to check
   * @return true if enough accumulated permits
   */
  boolean immediatelyAvailable(int permits);
}
