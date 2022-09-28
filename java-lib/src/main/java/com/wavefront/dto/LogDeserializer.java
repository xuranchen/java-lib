package com.wavefront.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

import java.io.IOException;
import java.util.*;

/**
 * Deserialization override for the Log class.
 *
 */
public class LogDeserializer extends JsonDeserializer<Log> {

    @Override
    public Log deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);

        Set<String> specialFields = new HashSet<>(Arrays.asList("timestamp", "text", "source"));
        long timestamp = node.get("timestamp").longValue();
        String message = node.get("text").asText();

        String source = node.get("source").asText();
        List<Annotation> annotations = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (!specialFields.contains(field.getKey())) {
                annotations.add(new Annotation(field.getKey(), field.getValue().textValue()));
            }
        }


        return new Log(new ReportLog(timestamp, message, source, annotations));
    }
}
