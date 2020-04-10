package com.wavefront.integrations.metrics;

import com.tdunning.math.stats.Centroid;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.common.clients.WavefrontClientFactory;
import com.wavefront.sdk.entities.histograms.HistogramGranularity;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.WavefrontHistogram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Yammer MetricProcessor that sends metrics via an HttpClient. Provides support for sending to a secondary/backup
 * destination.
 * <p>
 * This sends a DIFFERENT metrics taxonomy than the Wavefront "dropwizard" metrics reporters.
 *
 * @author Mike McMahon (mike@wavefront.com)
 */
public class HttpMetricsProcessor extends WavefrontMetricsProcessor {

  private final Logger log = Logger.getLogger(HttpMetricsProcessor.class.getCanonicalName());

  private final Supplier<Long> timeSupplier;
  private final WavefrontSender wavefrontSender;

  private final String defaultSource;

  public static class Builder {
    private int queueSize = 50_000;
    private int batchSize = 10_000;
    private boolean prependGroupName = false;
    private boolean clear = false;
    private boolean sendZeroCounters = true;
    private boolean sendEmptyHistograms = true;
    private String defaultSource;
    private String hostname;
    private int metricsPort = 2878;
    private String secondaryHostname;
    private int secondaryPort = 2878;
    private Supplier<Long> timeSupplier = System::currentTimeMillis;

    public Builder withEndpoint(String hostname, int port) {
      this.hostname = hostname;
      this.metricsPort = port;
      return this;
    }

    public Builder withSecondaryEndpoint(String hostname, int port) {
      this.secondaryHostname = hostname;
      this.secondaryPort = port;
      return this;
    }

    public Builder withDefaultSource(String defaultSource) {
      this.defaultSource = defaultSource;
      return this;
    }

    public Builder withQueueOptions(int batchSize, int queueSize) {
      this.batchSize = batchSize;
      this.queueSize = queueSize;
      return this;
    }

    public Builder withPrependedGroupNames(boolean prependGroupName) {
      this.prependGroupName = prependGroupName;
      return this;
    }

    public Builder clearHistogramsAndTimers(boolean clear) {
      this.clear = clear;
      return this;
    }

    public Builder sendZeroCounters(boolean send) {
      this.sendZeroCounters = send;
      return this;
    }

    public Builder sendEmptyHistograms(boolean send) {
      this.sendEmptyHistograms = send;
      return this;
    }

    public Builder withTimeSupplier(Supplier<Long> timeSupplier) {
      this.timeSupplier = timeSupplier;
      return this;
    }

    public HttpMetricsProcessor build() {
      if (this.batchSize > this.queueSize)
        throw new IllegalArgumentException("Batch size cannot be larger than queue sizes");

      return new HttpMetricsProcessor(this);
    }
  }

  HttpMetricsProcessor(Builder builder) {
    super(builder.prependGroupName, builder.clear, builder.sendZeroCounters, builder.sendEmptyHistograms);
    this.timeSupplier = builder.timeSupplier;
    this.defaultSource = builder.defaultSource;

    WavefrontClientFactory factory = new WavefrontClientFactory();
    factory.addClient(
        "proxy://" + builder.hostname + ":" + builder.metricsPort,
        builder.batchSize, builder.queueSize, null, null);

    if (builder.secondaryHostname != null) {
      factory.addClient("proxy://" + builder.secondaryHostname + ":" + builder.secondaryPort,
          builder.batchSize, builder.queueSize, null, null);
    }
    this.wavefrontSender = factory.getClient();
  }

  @Override
  void writeMetric(MetricName name, String nameSuffix, double value) {
    Map<String, String> tags = Collections.emptyMap();
    if (name instanceof TaggedMetricName) {
      tags = ((TaggedMetricName) name).getTags();
    }
    try {
      String metricName = getName(name);
      if (nameSuffix != null && !nameSuffix.equals("")) {
        metricName += "." + nameSuffix;
      }
      wavefrontSender.sendMetric(metricName, value, timeSupplier.get() / 1000, defaultSource, tags);
    } catch (IOException ex) {
      log.log(Level.SEVERE, "Unable to forward point to the wavefront service", ex);
    }
  }

  @Override
  void writeHistogram(MetricName name, WavefrontHistogram histogram, Void context) {
    try {
      Map<String, String> tags = Collections.emptyMap();
      if (name instanceof TaggedMetricName) {
        tags = ((TaggedMetricName) name).getTags();
      }
      List<WavefrontHistogram.MinuteBin> bins = histogram.bins(clear);
      if (bins.isEmpty()) return;

      Set<HistogramGranularity> granularities = new HashSet<>();
      granularities.add(HistogramGranularity.MINUTE);
      long timestamp;
      for (WavefrontHistogram.MinuteBin bin : bins) {
        List<Pair<Double, Integer>> centroids = new ArrayList<>();
        timestamp = bin.getMinMillis() / 1000;
        Centroid accumulator = null;
        for (Centroid c : bin.getDist().centroids()) {
          if (accumulator != null && c.mean() != accumulator.mean()) {
            centroids.add(new Pair<>(accumulator.mean(), accumulator.count()));
            accumulator = new Centroid(c.mean(), c.count());
          } else {
            if (accumulator == null) {
              accumulator = new Centroid(c.mean(), c.count());
            } else {
              accumulator.add(c.mean(), c.count());
            }
          }
        }
        if (accumulator != null) {
          centroids.add(new Pair<>(accumulator.mean(), accumulator.count()));
        }
        wavefrontSender.sendDistribution(getName(name), centroids, granularities, timestamp, defaultSource, tags);
      }
    } catch (IOException ex) {
      log.log(Level.SEVERE, "Unable to forward histogram to the wavefront service", ex);
    }
  }

  @Override
  void flush() {
    try {
      wavefrontSender.flush();
    } catch (IOException ex) {
      log.log(Level.SEVERE, "Failed to flush clients", ex);
    }
  }
}
