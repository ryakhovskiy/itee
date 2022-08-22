package org.kr.itee.perfmon.ws.web;

import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.junit.Test;

/**
 *
 */
public class RequestHandlerTest {

    @Test
    public void startAutorun() throws Exception {
        RequestHandler handler = new RequestHandler();
        String json = IOUtils.getInstance().getResourceAsString("request.json");
        handler.startAutorun(json);
    }

}