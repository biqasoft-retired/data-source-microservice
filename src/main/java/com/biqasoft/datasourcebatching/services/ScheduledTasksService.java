/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.services;

import com.biqasoft.datasourcebatching.batching.LeadGenProcessingService;
import com.biqasoft.datasourcebatching.customers.ProcessDynamicSegmentsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Created by Nikita Bakaev, ya@nbakaev.ru on 12/26/2015.
 * All Rights Reserved
 */
@Service
public class ScheduledTasksService {

    @Autowired
    private LeadGenProcessingService domainRepository;

    @Autowired
    private KPIService KPIService;

    @Autowired
    private ProcessDynamicSegmentsService processDynamicSegmentsService;

    private static final Logger logger = LoggerFactory.getLogger(ScheduledTasksService.class);

    @Scheduled(cron = "0 0/5 0,2-23 * * ?")
    public void scheduledLeadGen (){
        logger.debug("START: `leadGenMethod and projects KPIs`");
        domainRepository.createAllDS();
        logger.debug("END: `leadGenMethod and projects KPIs`");
    }

    @Scheduled(cron = "0 0/5 0,2-23 * * ?")
    public void scheduledDataSource() {
        logger.debug("START: `data source KPIs`");
        KPIService.processAllDomains();
        logger.debug("END: `data source KPIs`");
    }

    @Scheduled(cron = "0 0/5 0,2-23 * * ?")
    public void createAllDynamicSegmentsCustomers() {
        logger.debug("START: `createAllDynamicSegmentsCustomers`");
        processDynamicSegmentsService.createAllDynamicSegmentsCustomers();
        logger.debug("END: `createAllDynamicSegmentsCustomers`");
    }

}
