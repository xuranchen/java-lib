package com.wavefront.ingester;

import org.junit.Test;
import wavefront.report.ReportSourceTag;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ReportSourceTagSerializerTest {

  @Test
  public void testSerializer() {
    ReportSourceTagSerializer serializer = new ReportSourceTagSerializer();
    ReportSourceTagDecoder decoder = new ReportSourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    decoder.decode("@SourceTag action=delete source=aSource sourceTag4 sourceTag5", out);
    assertEquals(2, out.size());
    assertEquals("@SourceTag action=delete source=\"aSource\" \"sourceTag4\"",
        serializer.apply(out.get(0)));
    assertEquals("@SourceTag action=delete source=\"aSource\" \"sourceTag5\"",
        serializer.apply(out.get(1)));
  }
}