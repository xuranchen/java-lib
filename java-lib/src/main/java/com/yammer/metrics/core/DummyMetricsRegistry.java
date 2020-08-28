package com.yammer.metrics.core;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

/**
 * A no-op implementation of a metrics registry.
 *
 * @author vasily@wavefront.com
 */
public class DummyMetricsRegistry extends MetricsRegistry {
  private static final ScheduledExecutorService DUMMY_EXECUTOR = new ScheduledThreadPoolExecutor(1);
  private static final Gauge<Object> DUMMY_GAUGE = new Gauge<Object>() {
    @Override
    public Object value() {
      return null;
    }
  };
  private static final Counter DUMMY_COUNTER = new Counter();
  private static final Histogram DUMMY_HISTOGRAM = new Histogram(Histogram.SampleType.BIASED);
  private static final Timer DUMMY_TIMER = new Timer(DUMMY_EXECUTOR, TimeUnit.MILLISECONDS,
      TimeUnit.SECONDS);
  private static final Meter DUMMY_METER = new Meter(DUMMY_EXECUTOR, "noop", TimeUnit.SECONDS,
      Clock.defaultClock());

  @SuppressWarnings("unchecked")
  @Override
  public <T> Gauge<T> newGauge(Class<?> klass, String name, Gauge<T> metric) {
    return (Gauge<T>) DUMMY_GAUGE;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Gauge<T> newGauge(Class<?> klass, String name, String scope, Gauge<T> metric) {
    return (Gauge<T>) DUMMY_GAUGE;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Gauge<T> newGauge(MetricName metricName, Gauge<T> metric) {
    return (Gauge<T>) DUMMY_GAUGE;
  }

  @Override
  public Counter newCounter(Class<?> klass, String name) {
    return DUMMY_COUNTER;
  }

  @Override
  public Counter newCounter(Class<?> klass, String name, String scope) {
    return DUMMY_COUNTER;
  }

  @Override
  public Counter newCounter(MetricName metricName) {
    return DUMMY_COUNTER;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name, boolean biased) {
    return DUMMY_HISTOGRAM;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name, String scope, boolean biased) {
    return DUMMY_HISTOGRAM;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name) {
    return DUMMY_HISTOGRAM;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name, String scope) {
    return DUMMY_HISTOGRAM;
  }

  @Override
  public Histogram newHistogram(MetricName metricName, boolean biased) {
    return DUMMY_HISTOGRAM;
  }

  @Override
  public Meter newMeter(Class<?> klass, String name, String eventType, TimeUnit unit) {
    return DUMMY_METER;
  }

  @Override
  public Meter newMeter(Class<?> klass, String name, String scope, String eventType,
                        TimeUnit unit) {
    return DUMMY_METER;
  }

  @Override
  public Meter newMeter(MetricName metricName, String eventType, TimeUnit unit) {
    return DUMMY_METER;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name) {
    return DUMMY_TIMER;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return DUMMY_TIMER;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name, String scope) {
    return DUMMY_TIMER;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name, String scope, TimeUnit durationUnit, TimeUnit rateUnit) {
    return DUMMY_TIMER;
  }

  @Override
  public Timer newTimer(MetricName metricName, TimeUnit durationUnit, TimeUnit rateUnit) {
    return DUMMY_TIMER;
  }

  @Override
  public Map<MetricName, Metric> allMetrics() {
    return ImmutableMap.of();
  }

  @Override
  public SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics() {
    return ImmutableSortedMap.of();
  }

  @Override
  public SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics(MetricPredicate predicate) {
    return ImmutableSortedMap.of();
  }

  @Override
  public void removeMetric(Class<?> klass, String name) {
  }

  @Override
  public void removeMetric(Class<?> klass, String name, String scope) {
  }

  @Override
  public void removeMetric(MetricName name) {
  }

  @Override
  public void addListener(MetricsRegistryListener listener) {
  }

  @Override
  public void removeListener(MetricsRegistryListener listener) {
  }
}
