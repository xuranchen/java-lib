package com.wavefront.dto;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.Instant;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.wavefront.common.SerializerUtils.appendQuoted;

/**
 * Serializing wrapper for the Log class.
 *
 * @author amitw@vmware.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Log implements Serializable {

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("text")
    private String message;

    @JsonProperty()
    private String source;

    @JsonProperty("log_level")
    private String level;

    @JsonProperty("error_name")
    private String exception;

    private Map<String, String> annotations;

    @JsonIgnore
    private int dataSize;

    @SuppressWarnings("unused")
    private Log() { }

    public Log(ReportLog log) {
        this.timestamp = log.getTimestamp();
        this.message = log.getMessage();
        this.source = log.getHost();
        this.level = log.getLevel();
        this.exception = log.getException();
        this.annotations = new HashMap<>();
        for (Annotation tag : log.getAnnotations()) {
            annotations.put(tag.getKey(), tag.getValue());
        }
        this.dataSize = this.toString().length();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

    public String getSource() { return source; }

    public int getDataSize() {
        return dataSize;
    }

    public String getLevel() {
        return level;
    }

    public String getException() {
        return exception;
    }

    @JsonAnyGetter
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = result * 31 + (int) (timestamp ^ (timestamp >>> 32));
        result = result * 31 + (message == null ? 0 : message.hashCode());
        result = result * 31 + (source == null ? 0 : source.hashCode());
        result = result * 31 + (level == null ? 0 : level.hashCode());
        result = result * 31 + (exception == null ? 0 : exception.hashCode());
        result = result * 31 + annotations.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Log other = (Log) obj;
        if (timestamp != other.timestamp) return false;
        if (!Objects.equals(message, other.message)) return false;
        if (!Objects.equals(source, other.source)) return false;
        if (!Objects.equals(level, other.level)) return false;
        if (!Objects.equals(exception, other.exception)) return false;
        if (!annotations.equals(other.annotations)) return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (String key : annotations.keySet()) {
            appendQuoted(sb, key);
            sb.append(":");
            appendQuoted(sb, annotations.get(key));
            sb.append(", ");
        }
        appendQuoted(sb, "timestamp");
        sb.append(":");
        sb.append(timestamp);
        sb.append(", ");
        appendQuoted(sb, "text");
        sb.append(":");
        appendQuoted(sb, message);
        sb.append(", ");
        appendQuoted(sb, "source");
        sb.append(":");
        appendQuoted(sb, source);
        sb.append(", ");
        appendQuoted(sb, "log_level");
        sb.append(":");
        appendQuoted(sb, level);
        sb.append(", ");
        appendQuoted(sb, "error_name");
        sb.append(":");
        appendQuoted(sb, exception);
        sb.append("}");
        return sb.toString();
    }
}
