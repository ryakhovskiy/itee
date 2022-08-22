package org.kr.itee.perfmon.ws.web.rest;

import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.kr.itee.perfmon.ws.web.RequestHandler;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;

/**
 *
 */
@Path("/perfmon")
public class PerfmonRSWS {

    private static final RequestHandler REQUEST_HANDLER = new RequestHandler();

    private final Logger log = Logger.getLogger(PerfmonRSWS.class);
    private final boolean isDebugEnabled = log.isDebugEnabled();


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/autorun/start")
    public Response start(InputStream inputStream) {
        try {
            String request = IOUtils.getInstance().readString(inputStream);
            if (isDebugEnabled)
                log.info("request received: " + request);
            int id = REQUEST_HANDLER.startAutorun(request);
            return Response.status(Response.Status.OK).entity(String.valueOf(id)).build();
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    @POST
    @Path("/autorun/stop/{id}")
    public Response stop(@PathParam("id") String id) {
        try {
            int iid = Integer.parseInt(id);
            boolean stopped = REQUEST_HANDLER.stopAutorun(iid);
            if (stopped)
                return Response.status(Response.Status.OK).build();
            else
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    @POST
    @Path("/autorun/stopall")
    public Response stopAll() {
        try {
            REQUEST_HANDLER.stopAll();
            return Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

}
