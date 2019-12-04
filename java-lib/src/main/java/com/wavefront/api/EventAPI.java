package com.wavefront.api;

import com.wavefront.dto.Event;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;

@Path("/")
public interface EventAPI {

  /**
   * Ingest a batch of events.
   *
   * @param eventBatch batch of events to be reported
   * @return HTTP response
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("v2/wfproxy/event")
  Response proxyEvents(@HeaderParam("X-WF-PROXY-ID") final UUID proxyId,
                       final List<Event> eventBatch);
}
