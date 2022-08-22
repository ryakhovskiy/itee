package org.kr.intp.webservice;

import org.kr.intp.config.IntpConfigurationController;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 */
@Path("/communication")
public class WebServiceTest {

    @Inject
    IntpConfigurationController configurationController;

    @GET
    public String hello() {
        return "Web Service working. Sample value from config (host): " + configurationController.getConfig().getDbHost();
    }
}
