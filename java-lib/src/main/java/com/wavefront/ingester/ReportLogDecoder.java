package com.wavefront.ingester;


import wavefront.report.ReportLog;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

public class ReportLogDecoder implements ReportableEntityDecoder<String, ReportLog> {
    private static final AbstractIngesterFormatter<ReportLog> FORMAT =
            ReportLogIngesterFormatter.newBuilder().build();


    private final Supplier<String> hostNameSupplier;
    private List<String> customSourceTags;
    private List<String> customLogTimestampTags;
    private List<String> customLogMessageTags;
    private List<String> customApplicationTags;
    private List<String> customServiceTags;
    private List<String> customLevelTags;
    private List<String> customExceptionTags;


    public ReportLogDecoder(@Nullable Supplier<String> hostNameSupplier,
                            List<String> customSourceTags, List<String> customLogTimestampTags, List<String> customLogMessageTags,
                            List<String> customApplicationTags, List<String> customServiceTags) {
        this.hostNameSupplier = hostNameSupplier;
        this.customSourceTags = customSourceTags;
        this.customLogTimestampTags = customLogTimestampTags;
        this.customLogMessageTags = customLogMessageTags;
        this.customApplicationTags = customApplicationTags;
        this.customServiceTags = customServiceTags;
    }

    public ReportLogDecoder(@Nullable Supplier<String> hostNameSupplier,
                            List<String> customSourceTags, List<String> customLogTimestampTags, List<String> customLogMessageTags,
                            List<String> customApplicationTags, List<String> customServiceTags,
                            List<String> customLevelTags, List<String> customExceptionTags) {
        this.hostNameSupplier = hostNameSupplier;
        this.customSourceTags = customSourceTags;
        this.customLogTimestampTags = customLogTimestampTags;
        this.customLogMessageTags = customLogMessageTags;
        this.customApplicationTags = customApplicationTags;
        this.customServiceTags = customServiceTags;
        this.customLevelTags = customLevelTags;
        this.customExceptionTags = customExceptionTags;
    }


    @Override
    public void decode(String msg, List<ReportLog> out, String customerId, @Nullable IngesterContext ctx) {
        ReportLog log = FORMAT.drive(msg, hostNameSupplier, "default", customSourceTags, customLogTimestampTags,
                customLogMessageTags, customApplicationTags, customServiceTags, customLevelTags, customExceptionTags, ctx);
        if (out != null) {
            out.add(log);
        }
    }
}
