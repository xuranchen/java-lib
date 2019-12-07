package com.wavefront.api;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * API for source tag operations.
 *
 * @author vasily@wavefront.com
 */
@Path("/")
public interface SourceTagAPI {

  /**
   * Add a single tag to a source.
   *
   * @param id       source ID.
   * @param tagValue tag to add.
   */
  @PUT
  @Path("v2/source/{id}/tag/{tagValue}")
  @Produces(MediaType.APPLICATION_JSON)
  Response appendTag(@PathParam("id") String id,
                     @PathParam("tagValue") String tagValue);

  /**
   * Remove a single tag from a source.
   *
   * @param id       source ID.
   * @param tagValue tag to remove.
   */
  @DELETE
  @Path("v2/source/{id}/tag/{tagValue}")
  @Produces(MediaType.APPLICATION_JSON)
  Response removeTag(@PathParam("id") String id,
                     @PathParam("tagValue") String tagValue);

  /**
   * Sets tags for a host, overriding existing tags.
   *
   * @param id             source ID.
   * @param tagValuesToSet tags to set.
   */
  @POST
  @Path("v2/source/{id}/tag")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  Response setTags(@PathParam ("id") String id,
                   List<String> tagValuesToSet);

  /**
   * Set description for a source.
   *
   * @param id          source ID.
   * @param description description.
   */
  @POST
  @Path("v2/source/{id}/description")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  Response setDescription(@PathParam("id") String id,
                          String description);

  /**
   * Remove description from a source.
   *
   * @param id    source ID.
   */
  @DELETE
  @Path("v2/source/{id}/description")
  @Produces(MediaType.APPLICATION_JSON)
  Response removeDescription(@PathParam("id") String id);
}
