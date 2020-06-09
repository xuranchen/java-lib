package com.wavefront.api.json;

import org.joda.time.Instant;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Marshaller for Joda Instant to JSON and back.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class InstantMarshaller {

  public static class Serializer extends JsonSerializer<Instant> {

    @Override
    public void serialize(Instant value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      jgen.writeNumber(value.getMillis());
    }
  }


  public static class Deserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      return new Instant(jp.getLongValue());
    }
  }
}
