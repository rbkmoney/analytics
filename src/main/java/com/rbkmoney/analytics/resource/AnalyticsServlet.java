package com.rbkmoney.analytics.resource;

import com.rbkmoney.damsel.analytics.AnalyticsServiceSrv;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import lombok.RequiredArgsConstructor;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;

import java.io.IOException;

@WebServlet("/analytics/v1")
@RequiredArgsConstructor
public class AnalyticsServlet extends GenericServlet {

    private final AnalyticsServiceSrv.Iface analyticsHandler;
    private Servlet thriftServlet;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(AnalyticsServiceSrv.Iface.class, analyticsHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}
