package com.wavefront.ingester;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Ingestion formatter for logs.
 *
 * @author amitw@vmware.com
 */
public class ReportLogIngesterFormatter extends AbstractIngesterFormatter<ReportLog>  {

    private ReportLogIngesterFormatter(List<FormatterElement<ReportLog>> elements) {
        super(elements);
    }

    public static class ReportLogIngesterFormatBuilder extends IngesterFormatBuilder<ReportLog> {
        @Override
        public ReportLogIngesterFormatter build() {
            return new ReportLogIngesterFormatter(elements);
        }
    }

    public static IngesterFormatBuilder<ReportLog> newBuilder() {
        return new ReportLogIngesterFormatter.ReportLogIngesterFormatBuilder();
    }

    @Override
    public ReportLog drive(String logJson, @Nullable Supplier<String> defaultHostNameSupplier,
                           String customerId, @Nullable List<String> customSourceTags, @Nullable List<String> customLogTimestampTags,
                           @Nullable List<String> customLogMessageTags, List<String> customLogApplicationTags, List<String> customLogServiceTags,
                           @Nullable List<String> customLogLevelTags,@Nullable List<String> customLogExceptionTags, @Nullable IngesterContext ingesterContext) {
        final ReportLog log = new ReportLog();
        List<Annotation> annotations = new ArrayList<>();

        try {
            Map<String, Object> tagMap = new ObjectMapper().readValue(logJson, new TypeReference<Map<String,Object>>(){});
            List<String> logMessageTags = customLogMessageTags == null ? AbstractIngesterFormatter.getDefaultLogMessageKeys() :
                    Stream.concat(AbstractIngesterFormatter.getDefaultLogMessageKeys().stream(), customLogMessageTags.stream()).collect(Collectors.toList());
            JsonParser parser = new JsonParser(tagMap, logMessageTags);
            parser.flattenJson(annotations);
            log.setAnnotations(annotations);
            String host = AbstractIngesterFormatter.getHost(log.getAnnotations(), customSourceTags);
            if (host == null) {
                if (defaultHostNameSupplier == null) {
                    host = "unknown";
                } else {
                    host = defaultHostNameSupplier.get();
                }
            }
            log.setHost(host);
            Long timestamp = AbstractIngesterFormatter.getLogTimestamp(log.getAnnotations(), customLogTimestampTags);
            log.setTimestamp(timestamp);
            String message = AbstractIngesterFormatter.getLogMessage(log.getAnnotations(), customLogMessageTags);
            log.setMessage(message);
            String application = AbstractIngesterFormatter.getLogApplication(log.getAnnotations(), customLogApplicationTags);
            log.setApplication(application);
            String service = AbstractIngesterFormatter.getLogService(log.getAnnotations(), customLogServiceTags);
            log.setService(service);
            String level = AbstractIngesterFormatter.getLogLevel(log.getAnnotations(), customLogLevelTags);
            log.setLevel(level);
            String exception = AbstractIngesterFormatter.getLogException(log.getAnnotations(), customLogExceptionTags);
            log.setException(exception);
            return log;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }



    }
}
