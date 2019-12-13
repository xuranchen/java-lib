package com.google.common.util.concurrent;

import com.wavefront.common.LazySupplier;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * A decorator for {@link RecyclableRateLimiter} that adds metrics for permits-related operations.
 *
 * @author vasily@wavefront.com
 */
public class RecyclableRateLimiterWithMetrics implements RecyclableRateLimiter {

  private final RecyclableRateLimiter delegate;
  private final Supplier<RateLimiterMetricsContainer> metrics;

  /**
   * @param delegate {@link RecyclableRateLimiter} delegate
   * @param prefix   metric prefix
   */
  public RecyclableRateLimiterWithMetrics(RecyclableRateLimiter delegate,
                                          String prefix) {
    this(delegate, LazySupplier.of(() -> new RateLimiterMetricsContainer(
        Metrics.newCounter(new TaggedMetricName(prefix, "permits-granted")),
        Metrics.newCounter(new TaggedMetricName(prefix, "permits-denied")),
        Metrics.newCounter(new TaggedMetricName(prefix, "permits-retried"))
    )));
  }

  /**
   * @param delegate {@link RecyclableRateLimiter} delegate
   * @param metrics  a supplier with
   */
  public RecyclableRateLimiterWithMetrics(RecyclableRateLimiter delegate,
                                          Supplier<RateLimiterMetricsContainer> metrics) {
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public double getRate() {
    return delegate.getRate();
  }

  @Override
  public void setRate(double rate) {
    delegate.setRate(rate);
  }

  @Override
  public double acquire(int permits) {
    double result = delegate.acquire(permits);
    Counter granted = metrics.get().permitsGrantedCounter;
    if (granted != null) granted.inc(permits);
    return result;
  }

  @Override
  public boolean tryAcquire(int permits) {
    if (delegate.tryAcquire(permits)) {
      Counter granted = metrics.get().permitsGrantedCounter;
      if (granted != null) granted.inc(permits);
      return true;
    } else {
      Counter denied = metrics.get().permitsDeniedCounter;
      if (denied != null) denied.inc(permits);
      return false;
    }
  }

  @Override
  public void recyclePermits(int permits) {
    delegate.recyclePermits(permits);
    Counter recycled = metrics.get().permitsRecycledCounter;
    if (recycled != null) recycled.inc(permits);
  }

  @Override
  public boolean immediatelyAvailable(int permits) {
    return delegate.immediatelyAvailable(permits);
  }

  public static class RateLimiterMetricsContainer {
    private Counter permitsGrantedCounter;
    private Counter permitsDeniedCounter;
    private Counter permitsRecycledCounter;
    private RateLimiterMetricsContainer(@Nullable Counter permitsGrantedCounter,
                                        @Nullable Counter permitsDeniedCounter,
                                        @Nullable Counter permitsRecycledCounter) {
      this.permitsGrantedCounter = permitsGrantedCounter;
      this.permitsDeniedCounter = permitsDeniedCounter;
      this.permitsRecycledCounter = permitsRecycledCounter;
    }
  }
}
