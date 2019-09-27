package com.google.common.util.concurrent;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;

/**
 * A decorator for {@link RecyclableRateLimiter} that adds metrics for permits-related operations.
 *
 * @author vasily@wavefront.com
 */
public class RecyclableRateLimiterWithMetrics implements RecyclableRateLimiter {

  private final RecyclableRateLimiter rateLimiter;
  private final Counter permitsGranted;
  private final Counter permitsDenied;
  private final Counter permitsRecycled;

  /**
   * @param rateLimiter            RecyclableRateLimiter delegate
   * @param permitsGrantedCounter  Counter tracking the number of permits granted
   * @param permitsDeniedCounter   Counter tracking the number of permits denied
   * @param permitsRecycledCounter Counter tracking the number of permits returned to the pool
   */
  public RecyclableRateLimiterWithMetrics(RecyclableRateLimiter rateLimiter,
                                          @Nullable Counter permitsGrantedCounter,
                                          @Nullable Counter permitsDeniedCounter,
                                          @Nullable Counter permitsRecycledCounter) {
    this.rateLimiter = rateLimiter;
    this.permitsGranted = permitsGrantedCounter;
    this.permitsDenied = permitsDeniedCounter;
    this.permitsRecycled = permitsRecycledCounter;
  }

  @Override
  public double getRate() {
    return rateLimiter.getRate();
  }

  @Override
  public void setRate(double rate) {
    rateLimiter.setRate(rate);
  }

  @Override
  public double acquire(int permits) {
    double result = rateLimiter.acquire(permits);
    if (permitsGranted != null) permitsGranted.inc(permits);
    return result;
  }

  @Override
  public boolean tryAcquire(int permits) {
    if (rateLimiter.tryAcquire(permits)) {
      if (permitsGranted != null) permitsGranted.inc(permits);
      return true;
    } else {
      if (permitsDenied != null) permitsDenied.inc(permits);
      return false;
    }
  }

  @Override
  public void recyclePermits(int permits) {
    rateLimiter.recyclePermits(permits);
    if (permitsRecycled != null) permitsRecycled.inc(permits);
  }

  @Override
  public boolean immediatelyAvailable(int permits) {
    boolean result = rateLimiter.immediatelyAvailable(permits);
    if (!result) {
      if (permitsDenied != null) permitsDenied.inc(permits);
    }
    return result;
  }
}
