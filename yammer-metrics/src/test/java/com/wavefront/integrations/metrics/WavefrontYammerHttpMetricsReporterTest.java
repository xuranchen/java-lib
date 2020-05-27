package com.wavefront.integrations.metrics;

import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.MetricTranslator;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.WavefrontHistogram;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.impl.nio.bootstrap.HttpServer;
import org.apache.http.impl.nio.bootstrap.ServerBootstrap;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestConsumer;
import org.apache.http.nio.protocol.HttpAsyncExchange;
import org.apache.http.nio.protocol.HttpAsyncRequestConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestHandler;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.hamcrest.text.MatchesPattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

/**
 * @author Mike McMahon (mike@wavefront.com)
 */
public class WavefrontYammerHttpMetricsReporterTest {

  private MetricsRegistry metricsRegistry;
  private WavefrontYammerHttpMetricsReporter wavefrontYammerHttpMetricsReporter;
  private HttpServer metricsServer;
  private List<String> inputMetrics;
  private Long stubbedTime = 1485224035000L;

  private void innerSetUp(boolean prependGroupName, MetricTranslator metricTranslator,
                          boolean includeJvmMetrics, boolean includeReporterMetrics, boolean clear) throws IOException {

    metricsRegistry = new MetricsRegistry();
    inputMetrics = new ArrayList<>();

    if (metricsServer == null) {
      IOReactorConfig metricsIOreactor = IOReactorConfig.custom().
          setTcpNoDelay(true).
          setIoThreadCount(10).
          setSelectInterval(200).
          build();
      metricsServer = ServerBootstrap.bootstrap().
          setLocalAddress(InetAddress.getLoopbackAddress()).
          setListenerPort(0).
          setServerInfo("Test/1.1").
          setIOReactorConfig(metricsIOreactor).
          registerHandler("*", new HttpAsyncRequestHandler<HttpRequest>() {
            @Override
            public HttpAsyncRequestConsumer<HttpRequest> processRequest(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
              return new BasicAsyncRequestConsumer();
            }

            @Override
            public void handle(HttpRequest httpRequest, HttpAsyncExchange httpAsyncExchange, HttpContext httpContext) throws HttpException, IOException {
              if (httpRequest instanceof BasicHttpEntityEnclosingRequest) {
                HttpEntity entity = ((BasicHttpEntityEnclosingRequest) httpRequest).getEntity();
                InputStream fromMetrics;
                Header[] headers = httpRequest.getHeaders(HTTP.CONTENT_ENCODING);
                boolean gzip = false;
                for (Header header : headers) {
                  if (header.getValue().equals("gzip")) {
                    gzip = true;
                    break;
                  }
                }
                if (gzip)
                  fromMetrics = new GZIPInputStream(entity.getContent());
                else
                  fromMetrics = entity.getContent();

                int c = 0;
                while (c != -1) {
                  ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                  while ((c = fromMetrics.read()) != '\n' && c != -1) {
                    outputStream.write(c);
                  }
                  String metric = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
                  if (StringUtils.isEmpty(metric))
                    continue;
                  inputMetrics.add(metric);
                }
              }
              // Send an OK response
              httpAsyncExchange.submitResponse();
            }
          }).
          create();
      metricsServer.start();
      try {
        // Allow time for the Async HTTP Servers to bind
        Thread.sleep(50);
      } catch (InterruptedException ex) {
        throw new RuntimeException("Interrupted trying to sleep.", ex);
      }
    }
    wavefrontYammerHttpMetricsReporter = new WavefrontYammerHttpMetricsReporter.Builder().
        withName("test-http").
        withMetricsRegistry(metricsRegistry).
        withEndpoint(InetAddress.getLoopbackAddress().getHostAddress(), ((InetSocketAddress) metricsServer.getEndpoint().getAddress()).getPort()).
        withTimeSupplier(() -> stubbedTime).
        withMetricTranslator(metricTranslator).
        withPrependedGroupNames(prependGroupName).
        clearHistogramsAndTimers(clear).
        includeJvmMetrics(includeJvmMetrics).
        includeReporterMetrics(includeReporterMetrics).
        withDefaultSource("test").
        build();
  }

  @Before
  public void setUp() throws Exception {
    innerSetUp(false, null, false, false, false);
  }

  @After
  public void tearDown() throws IOException, InterruptedException{
    this.wavefrontYammerHttpMetricsReporter.shutdown(1, TimeUnit.MILLISECONDS);
    for (Map.Entry<MetricName, Metric> entry : this.metricsRegistry.allMetrics().entrySet()) {
      this.metricsRegistry.removeMetric(entry.getKey());
    }
    inputMetrics.clear();
  }

  @Test(timeout = 2000)
  public void testJvmMetrics() throws Exception {
    innerSetUp(true, null, true, false, false);
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertThat(inputMetrics, not(hasItem(MatchesPattern.matchesPattern("\".* .*\".* source=\"test\""))));
    assertThat(inputMetrics, hasItem(startsWith("\"jvm.memory.heapCommitted\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"jvm.fd_usage\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"jvm.buffers.mapped.totalCapacity\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"jvm.buffers.direct.totalCapacity\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"jvm.thread-states.runnable\"")));
  }

  @Test(timeout = 2000)
  public void testReporterMetrics() throws Exception {
    innerSetUp(true, null, false, true, false);
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertThat(inputMetrics, not(hasItem(MatchesPattern.matchesPattern("\".* .*\".* source=\"test\""))));
    assertThat(inputMetrics, hasItem(startsWith("\"java-lib.metrics.http.yammer-metrics.failed\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"java-lib.metrics.http.yammer-metrics.points.generated\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"java-lib.metrics.http.yammer-metrics.histos.generated\"")));
  }

  @Test(timeout = 2000)
  public void testSendPartialBundle() throws Exception {
    innerSetUp(true, null, false, true, false);
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertEquals(1, wavefrontYammerHttpMetricsReporter.getMetricsFailedToSend());
    assertThat(inputMetrics, not(hasItem(MatchesPattern.matchesPattern("\".* .*\".* source=\"test\""))));
    assertThat(inputMetrics, hasItem(startsWith("\"java-lib.metrics.http.yammer-metrics.failed\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"java-lib.metrics.http.yammer-metrics.points.generated\"")));
    assertThat(inputMetrics, hasItem(startsWith("\"java-lib.metrics.http.yammer-metrics.histos.generated\"")));
  }

  @Test(timeout = 2000)
  public void testPlainCounter() throws Exception {
    Counter counter = metricsRegistry.newCounter(WavefrontYammerMetricsReporterTest.class, "mycount");
    counter.inc();
    counter.inc();
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertThat(inputMetrics, contains(equalTo("\"mycount\" 2.0 1485224035 source=\"test\"")));
  }

  @Test(timeout = 2000)
  public void testTransformer() throws Exception {
    innerSetUp(false, pair -> Pair.of(new TaggedMetricName(
        pair._1.getGroup(), pair._1.getName(), "tagA", "valueA"), pair._2), false, false, false);
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertThat(
        inputMetrics,
        contains(equalTo("\"mycounter\" 2.0 1485224035 source=\"test\" \"tagA\"=\"valueA\"")));
  }

  @Test(timeout = 2000)
  public void testTaggedCounter() throws Exception {
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertThat(
        inputMetrics,
        contains(equalTo("\"mycounter\" 2.0 1485224035 source=\"test\" \"tag1\"=\"value1\" \"tag2\"=\"value2\"")));
  }

  @Test(timeout = 2000)
  public void testPlainHistogramWithClear() throws Exception {
    innerSetUp(false, null, false, false, true);
    Histogram histogram = metricsRegistry.newHistogram(WavefrontYammerMetricsReporterTest.class, "myhisto");
    histogram.update(1);
    histogram.update(10);
    runReporter();
    assertThat(inputMetrics, hasSize(11));
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.min\" 1.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.max\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.mean\" 5.5 1485224035 source=\"test\""),
        equalTo("\"myhisto.sum\" 11.0 1485224035 source=\"test\""),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5 1485224035 source=\"test\""),
        equalTo("\"myhisto.p75\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p95\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p99\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p999\" 10.0 1485224035 source=\"test\"")
    ));
    // Second run should clear data.
    runReporter();
    assertThat(inputMetrics, hasItem("\"myhisto.count\" 0.0 1485224035 source=\"test\""));
  }

  @Test(timeout = 2000)
  public void testPlainHistogramWithoutClear() throws Exception {
    innerSetUp(false, null, false, false, false);
    Histogram histogram = metricsRegistry.newHistogram(WavefrontYammerMetricsReporterTest.class, "myhisto");
    histogram.update(1);
    histogram.update(10);
    runReporter();
    assertThat(inputMetrics, hasSize(11));
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.min\" 1.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.max\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.mean\" 5.5 1485224035 source=\"test\""),
        equalTo("\"myhisto.sum\" 11.0 1485224035 source=\"test\""),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5 1485224035 source=\"test\""),
        equalTo("\"myhisto.p75\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p95\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p99\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p999\" 10.0 1485224035 source=\"test\"")
    ));
    // Second run should be the same.
    runReporter();
    assertThat(inputMetrics, hasSize(11));
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"myhisto.count\" 2.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.min\" 1.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.max\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.mean\" 5.5 1485224035 source=\"test\""),
        equalTo("\"myhisto.sum\" 11.0 1485224035 source=\"test\""),
        startsWith("\"myhisto.stddev\""),
        equalTo("\"myhisto.median\" 5.5 1485224035 source=\"test\""),
        equalTo("\"myhisto.p75\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p95\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p99\" 10.0 1485224035 source=\"test\""),
        equalTo("\"myhisto.p999\" 10.0 1485224035 source=\"test\"")
    ));
  }

  @Test(timeout = 2000)
  public void testWavefrontHistogramThreaded() throws Exception {
    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    long timeBin = (clock.get() / 60000 * 60);
    final WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group", "myhisto", "tag1", "value1", "tag2", "value2"), 32, clock::get);

    ThreadPoolExecutor e = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
    Runnable updateHisto = () -> {
      for (int i = 0; i < 500; i++) {
        int[] samples = {100, 66, 37, 8, 7, 5, 1};
        for (int sample : samples) {
          if (i % sample == 0) {
            wavefrontHistogram.update(sample);
            break;
          }
        }
      }
    };
    e.execute(updateHisto);
    e.execute(updateHisto);
    e.execute(updateHisto);
    updateHisto.run();
    while (e.getCompletedTaskCount() < 3) {}

    // Advance the clock by 1 min ...
    clock.addAndGet(60000L + 1);

    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getHistogramsGenerated()));
    assertThat(inputMetrics, contains(
        equalTo("!M " + timeBin +
            " #1148 1.0 #276 5.0 #244 7.0 #232 8.0 #52 37.0 #28 66.0 #20 100.0 \"myhisto\" " +
            "source=\"test\" \"tag1\"=\"value1\" \"tag2\"=\"value2\"")
    ));
  }

  @Test
  public void testWavefrontHistogram() throws Exception {
    tearDown();
    innerSetUp(false, null,false,false, true);
    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    long timeBin = (clock.get() / 60000 * 60);
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group", "myhisto", "tag1", "value1", "tag2", "value2"), 32, clock::get);
    for (int i = 0; i < 500; i++) {
      int[] samples = {100, 66, 37, 8, 7, 5, 1};
      for (int sample : samples) {
        if (i % sample == 0) {
          wavefrontHistogram.update(sample);
          break;
        }
      }
    }

    // Advance the clock by 1 min ...
    clock.addAndGet(60000L + 1);

    runReporter();
    int histogramsGenerated = wavefrontYammerHttpMetricsReporter.getHistogramsGenerated();
    assertThat(inputMetrics, hasSize(histogramsGenerated));
    assertThat(inputMetrics, contains(equalTo("!M " + timeBin +
        " #287 1.0 #69 5.0 #61 7.0 #58 8.0 #13 37.0 #7 66.0 #5 100.0 \"myhisto\" source=\"test\" " +
        "\"tag1\"=\"value1\" \"tag2\"=\"value2\"")));

    timeBin = (clock.get() / 60000 * 60);
    for (int i = 0; i < 500; i++) {
      int[] samples = {100, 66, 37, 8, 7, 5, 1};
      for (int sample : samples) {
        if (i % sample == 0) {
          wavefrontHistogram.update(sample);
          break;
        }
      }
    }
    // Advance the clock by 1 min ...
    clock.addAndGet(60000L + 1);
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getHistogramsGenerated() - histogramsGenerated));
    assertThat(inputMetrics, contains(equalTo("!M " + timeBin +
        " #287 1.0 #69 5.0 #61 7.0 #58 8.0 #13 37.0 #7 66.0 #5 100.0 \"myhisto\" source=\"test\" " +
        "\"tag1\"=\"value1\" \"tag2\"=\"value2\"")));
  }

  @Test(timeout = 2000)
  public void testPlainMeter() throws Exception {
    Meter meter = metricsRegistry.newMeter(WavefrontYammerMetricsReporterTest.class, "mymeter", "requests",
        TimeUnit.SECONDS);
    meter.mark(42);
    runReporter();
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"mymeter.count\" 42.0 1485224035 source=\"test\""),
        startsWith("\"mymeter.mean\""),
        startsWith("\"mymeter.m1\""),
        startsWith("\"mymeter.m5\""),
        startsWith("\"mymeter.m15\"")
    ));
  }

  @Test(timeout = 2000)
  public void testPlainGauge() throws Exception {
    Gauge gauge = metricsRegistry.newGauge(
        WavefrontYammerMetricsReporterTest.class, "mygauge", new Gauge<Double>() {
          @Override
          public Double value() {
            return 13.0;
          }
        });
    runReporter();
    assertThat(inputMetrics, hasSize(wavefrontYammerHttpMetricsReporter.getMetricsGenerated()));
    assertThat(inputMetrics, contains(equalTo("\"mygauge\" 13.0 1485224035 source=\"test\"")));
  }

  @Test(timeout = 2000)
  public void testTimerWithClear() throws Exception {
    innerSetUp(false, null, false, false, true);
    Timer timer = metricsRegistry.newTimer(new TaggedMetricName("", "mytimer", "foo", "bar"),
        TimeUnit.SECONDS, TimeUnit.SECONDS);
    timer.time().stop();
    runReporter();
    assertThat(inputMetrics, hasSize(15));
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"mytimer.rate.count\" 1.0 1485224035 source=\"test\" \"foo\"=\"bar\""),
        startsWith("\"mytimer.duration.min\""),
        startsWith("\"mytimer.duration.max\""),
        startsWith("\"mytimer.duration.mean\""),
        startsWith("\"mytimer.duration.sum\""),
        startsWith("\"mytimer.duration.stddev\""),
        startsWith("\"mytimer.duration.median\""),
        startsWith("\"mytimer.duration.p75\""),
        startsWith("\"mytimer.duration.p95\""),
        startsWith("\"mytimer.duration.p99\""),
        startsWith("\"mytimer.duration.p999\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));

    runReporter();
    assertThat(inputMetrics, hasSize(15));
    assertThat(inputMetrics, hasItem("\"mytimer.rate.count\" 0.0 1485224035 source=\"test\" \"foo\"=\"bar\""));
  }

  @Test(timeout = 2000)
  public void testPlainTimerWithoutClear() throws Exception {
    innerSetUp(false, null, false, false, false);
    Timer timer = metricsRegistry.newTimer(WavefrontYammerMetricsReporterTest.class, "mytimer");
    timer.time().stop();
    runReporter();
    assertThat(inputMetrics, hasSize(15));
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"mytimer.rate.count\" 1.0 1485224035 source=\"test\""),
        startsWith("\"mytimer.duration.min\""),
        startsWith("\"mytimer.duration.max\""),
        startsWith("\"mytimer.duration.mean\""),
        startsWith("\"mytimer.duration.sum\""),
        startsWith("\"mytimer.duration.stddev\""),
        startsWith("\"mytimer.duration.median\""),
        startsWith("\"mytimer.duration.p75\""),
        startsWith("\"mytimer.duration.p95\""),
        startsWith("\"mytimer.duration.p99\""),
        startsWith("\"mytimer.duration.p999\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));

    // No changes.
    runReporter();
    assertThat(inputMetrics, hasSize(15));
    assertThat(inputMetrics, containsInAnyOrder(
        equalTo("\"mytimer.rate.count\" 1.0 1485224035 source=\"test\""),
        startsWith("\"mytimer.duration.min\""),
        startsWith("\"mytimer.duration.max\""),
        startsWith("\"mytimer.duration.mean\""),
        startsWith("\"mytimer.duration.sum\""),
        startsWith("\"mytimer.duration.stddev\""),
        startsWith("\"mytimer.duration.median\""),
        startsWith("\"mytimer.duration.p75\""),
        startsWith("\"mytimer.duration.p95\""),
        startsWith("\"mytimer.duration.p99\""),
        startsWith("\"mytimer.duration.p999\""),
        startsWith("\"mytimer.rate.m1\""),
        startsWith("\"mytimer.rate.m5\""),
        startsWith("\"mytimer.rate.m15\""),
        startsWith("\"mytimer.rate.mean\"")
    ));
  }

  @Test(timeout = 2000)
  public void testPrependGroupName() throws Exception {
    innerSetUp(true, null, false, false, false);

    // Counter
    TaggedMetricName taggedMetricName = new TaggedMetricName("group", "mycounter",
        "tag1", "value1", "tag2", "value2");
    Counter counter = metricsRegistry.newCounter(taggedMetricName);
    counter.inc();
    counter.inc();

    AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    long timeBin = (clock.get() / 60000 * 60);
    // Wavefront Histo
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry, new TaggedMetricName(
        "group3", "myhisto", "tag1", "value1", "tag2", "value2"), 32, clock::get);
    for (int i = 0; i < 500; i++) {
      int[] samples = {100, 66, 37, 8, 7, 5, 1};
      for (int sample : samples) {
        if (i % sample == 0) {
          wavefrontHistogram.update(sample);
          break;
        }
      }
    }

    // Exploded Histo
    Histogram histogram = metricsRegistry.newHistogram(new MetricName("group2", "", "myhisto"), false);
    histogram.update(1);
    histogram.update(10);

    // Advance the clock by 1 min ...
    clock.addAndGet(60000L + 1);

    runReporter();
    assertThat(inputMetrics, hasSize(13));
    assertThat(inputMetrics,
        containsInAnyOrder(
            equalTo("\"group.mycounter\" 2.0 1485224035 source=\"test\" \"tag1\"=\"value1\" \"tag2\"=\"value2\""),
            equalTo("\"group2.myhisto.count\" 2.0 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.min\" 1.0 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.max\" 10.0 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.mean\" 5.5 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.sum\" 11.0 1485224035 source=\"test\""),
            startsWith("\"group2.myhisto.stddev\""),
            equalTo("\"group2.myhisto.median\" 5.5 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.p75\" 10.0 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.p95\" 10.0 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.p99\" 10.0 1485224035 source=\"test\""),
            equalTo("\"group2.myhisto.p999\" 10.0 1485224035 source=\"test\""),
            equalTo("!M " + timeBin +
            " #287 1.0 #69 5.0 #61 7.0 #58 8.0 #13 37.0 #7 66.0 #5 100.0 \"group3.myhisto\" source=\"test\" " +
            "\"tag1\"=\"value1\" \"tag2\"=\"value2\"")));
  }

  private void runReporter() throws InterruptedException {
    inputMetrics.clear();
    wavefrontYammerHttpMetricsReporter.run();
    wavefrontYammerHttpMetricsReporter.flush();
  }
}
