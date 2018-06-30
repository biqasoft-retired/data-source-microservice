/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.batching;

import com.biqasoft.entity.constants.CUSTOMER_FIELDS;
import com.biqasoft.entity.core.CreatedInfo;
import com.biqasoft.entity.core.Domain;
import com.biqasoft.entity.customer.LeadGenMethod;
import com.biqasoft.entity.customer.LeadGenProject;
import com.biqasoft.entity.datasources.CacheableMetaInfo;
import com.biqasoft.entity.datasources.SavedLeadGenKPI;
import com.biqasoft.gateway.indicators.dto.IndicatorsDTO;
import com.biqasoft.microservice.common.MicroserviceDomain;
import com.biqasoft.microservice.database.MongoTenantHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class LeadGenProcessingService {

    @Autowired
    private LeadGenProjectKPIsRepository leadGenProjectKPIsRepository;

    @Autowired
    private LeadGenMethodKPIsRepository leadGenMethodKPIsRepository;

    @Autowired
    private MicroserviceDomain microserviceDomain;

    @Autowired
    private MongoTenantHelper mongoTenantHelper;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private ExecutorService service = Executors.newCachedThreadPool();

    private SavedLeadGenKPI processIndicatorKPIs(IndicatorsDTO indicatorsDAO, String objectId, MongoOperations mongoOperations) {
        SavedLeadGenKPI savedLeadGenKPI = new SavedLeadGenKPI();

        savedLeadGenKPI.setCachedKPIsData(indicatorsDAO);
        savedLeadGenKPI.setCreatedInfo(new CreatedInfo(new Date()));

        switch (indicatorsDAO.getType()) {
            case CUSTOMER_FIELDS.LEAD_GEN_METHOD:
                savedLeadGenKPI.setLeadGenMethodId(objectId);
                break;
            case CUSTOMER_FIELDS.LEAD_GEN_PROJECT:
                savedLeadGenKPI.setLeadGenProjectId(objectId);
                break;
        }

        mongoOperations.insert(savedLeadGenKPI);
        return savedLeadGenKPI;
    }

    private <T> void processLeadGenEntity(T object, MongoOperations mongoOperations) {
        IndicatorsDTO indicatorsDAO;

        if (object instanceof LeadGenProject) {
            LeadGenProject leadGenProject = (LeadGenProject) object;
            indicatorsDAO = leadGenProjectKPIsRepository.getIndicatorsForLeadGenProject(leadGenProject.getId(), mongoOperations);
            processIndicatorKPIs(indicatorsDAO, leadGenProject.getId(), mongoOperations);

            leadGenProject.setCachedKPIsData(indicatorsDAO);
            leadGenProject.setCachedKPIsMetaInfo(new CacheableMetaInfo(new Date()));
            mongoOperations.save(leadGenProject);

        } else if (object instanceof LeadGenMethod) {
            LeadGenMethod leadGenMethod = (LeadGenMethod) object;
            indicatorsDAO = leadGenMethodKPIsRepository.getIndicatorsForLeadGenProject(leadGenMethod.getId(), mongoOperations);
            processIndicatorKPIs(indicatorsDAO, leadGenMethod.getId(), mongoOperations);

            leadGenMethod.setCachedKPIsData(indicatorsDAO);
            leadGenMethod.setCachedKPIsMetaInfo(new CacheableMetaInfo(new Date()));
            mongoOperations.save(leadGenMethod);
        }
    }

    /**
     * process all domain's leadGen
     *
     * @param domain
     */
    private void processDomainLeadGen(String domain) {
        MongoOperations mongoOperations = mongoTenantHelper.domainDataBaseUnsafeGet(domain);

        // process all LeadGenProject
        List<LeadGenProject> leadGenProjects = mongoOperations.findAll(LeadGenProject.class);
        leadGenProjects.forEach(leadGenMethod -> processLeadGenEntity(leadGenMethod, mongoOperations));

        // process all LeadGenMethod
        List<LeadGenMethod> leadGenMethods = mongoOperations.findAll(LeadGenMethod.class);
        leadGenMethods.forEach(leadGenMethod -> processLeadGenEntity(leadGenMethod, mongoOperations));
    }

    /**
     * This method generate lead gen KPIs
     * such as LTV... CPC... ROI
     * and save to DB to print graphics in web
     */
    public void createAllDS() {
        List<Domain> domains = microserviceDomain.unsafeFindAllDomains();

        for (Domain domainInCRM : domains) {
            String domain = domainInCRM.getDomain();

            if (!domainInCRM.isActive()) {
                logger.info("Skipping inactive domain: " + domain);
                continue;
            }

            service.submit(() -> {
                try {
                    processDomainLeadGen(domain);
                } catch (Exception e) {
                    logger.error("processDomainLeadGen domain" + domain, e);
                }
            });
        }
    }

}
