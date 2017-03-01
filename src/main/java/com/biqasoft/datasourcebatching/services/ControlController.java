/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.services;

import com.biqasoft.datasourcebatching.batching.LeadGenProcessingService;
import com.biqasoft.datasourcebatching.customers.ProcessDynamicSegmentsService;
import com.biqasoft.datasourcebatching.datasource.DataSourceControllerAbstract;
import com.biqasoft.datasourcebatching.datasource.DataSourceResolving;
import com.biqasoft.entity.core.Domain;
import com.biqasoft.microservice.common.MicroserviceDomain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

/**
 * @author Nikita Bakaev, ya@nbakaev.ru
 *         Date: 10/5/2015
 *         All Rights Reserved
 */
@RestController
@RequestMapping("/main")
public class ControlController {

    private final LeadGenProcessingService domainRepository;
    private final KPIService KPIService;
    private final DataSourceResolving dataSourceResolving;
    private final ProcessDynamicSegmentsService processDynamicSegmentsService;
    private final MicroserviceDomain microserviceDomain;

    @Autowired
    public ControlController(KPIService KPIService, DataSourceResolving dataSourceResolving, ProcessDynamicSegmentsService processDynamicSegmentsService, MicroserviceDomain microserviceDomain, LeadGenProcessingService domainRepository) {
        this.KPIService = KPIService;
        this.dataSourceResolving = dataSourceResolving;
        this.processDynamicSegmentsService = processDynamicSegmentsService;
        this.microserviceDomain = microserviceDomain;
        this.domainRepository = domainRepository;
    }

    @RequestMapping(method = RequestMethod.GET)
    public List<Domain> findAllInDomainsUnsafe(HttpServletResponse response) {
        return microserviceDomain.unsafeFindAllDomains();
    }

    @RequestMapping(value = "ds", method = RequestMethod.GET)
    public void createAllDS(HttpServletResponse response) {
        domainRepository.createAllDS();
    }

    @RequestMapping(value = "data-source-save", method = RequestMethod.GET)
    public void createAllDSSavedData(HttpServletResponse response) {
        KPIService.processAllDomains();
    }

    @RequestMapping(value = "data-source-save/supported-controllers", method = RequestMethod.GET)
    public Map<String, DataSourceControllerAbstract> getAllSupportedControllers(HttpServletResponse response) {
        return dataSourceResolving.getClassesDataSource();
    }

    @RequestMapping(value = "customers/dynamic-segment", method = RequestMethod.GET)
    public void createAllDynamicSegmentsCustomers(HttpServletResponse response) {
        processDynamicSegmentsService.createAllDynamicSegmentsCustomers();
    }

}