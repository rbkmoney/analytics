package com.rbkmoney.analytics.resource;

import com.rbkmoney.damsel.analytics.AnalyticsServiceSrv;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import lombok.RequiredArgsConstructor;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet("/fraud_inspector/v1")
@RequiredArgsConstructor
public class AnalyticsServlet extends GenericServlet {

    private Servlet thriftServlet;

    private final AnalyticsServiceSrv.Iface iface;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(AnalyticsServiceSrv.Iface.class, iface);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}