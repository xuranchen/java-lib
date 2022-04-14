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
                           @Nullable List<String> customLogMessageTags, List<String> customLogApplicationTags, List<String> customLogServiceTags, @Nullable IngesterContext ingesterContext) {
        final ReportLog log = new ReportLog();
        List<Annotation> annotations = new ArrayList<>();

        try {
            Map<String, Object> tagMap = new ObjectMapper().readValue(logJson, new TypeReference<Map<String,Object>>(){});
            for (Map.Entry<String, Object> tagKV : tagMap.entrySet()) {
                String tagK = tagKV.getKey();
                String tagV = (tagKV.getValue() == null)? "null" : tagKV.getValue().toString();
                annotations.add(Annotation.newBuilder().setKey(tagK).setValue(tagV).build());
            }
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
            return log;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }



    }
}
