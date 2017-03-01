/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.customers;

import com.biqasoft.customer.CustomerFilterService;
import com.biqasoft.entity.core.Domain;
import com.biqasoft.entity.customer.Customer;
import com.biqasoft.entity.customer.DynamicSegment;
import com.biqasoft.entity.filters.CustomerFilter;
import com.biqasoft.entity.format.BiqaPaginationResultList;
import com.biqasoft.microservice.common.MicroserviceDomain;
import com.biqasoft.microservice.database.MongoTenantHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Calculate number of customers in every {@link DynamicSegment}
 * and set lastUpdateNumber, lastUpdate fields
 */
@Service
public class ProcessDynamicSegmentsService {

    private final CustomerFilterService customerFilterService;
    private final MongoTenantHelper mongoTenantHelper;
    private final MicroserviceDomain microserviceDomain;
    private static final Logger logger = LoggerFactory.getLogger(ProcessDynamicSegmentsService.class);

    private ExecutorService service = Executors.newCachedThreadPool();

    @Autowired
    public ProcessDynamicSegmentsService(MongoTenantHelper mongoTenantHelper, MicroserviceDomain microserviceDomain, CustomerFilterService customerFilterService) {
        this.mongoTenantHelper = mongoTenantHelper;
        this.microserviceDomain = microserviceDomain;
        this.customerFilterService = customerFilterService;
    }

    private void processCustomer(Map<String, Customer> customerMap, Customer customer, DynamicSegment dynamicSegment) {

        if (customerMap.containsKey(customer.getId())) {

        } else {
            customer.setDynamicSegments(new ArrayList<>());
            customerMap.put(customer.getId(), customer);
        }
        Customer customer1 = customerMap.get(customer.getId());
        customer1.getDynamicSegments().add(dynamicSegment.getId());
    }

    private void processCustomerAndUpdate(Map<String, Customer> customerMap, MongoOperations mongoOperations) {
        for (Map.Entry<String, Customer> entry : customerMap.entrySet()) {
            Customer value = entry.getValue();

            Criteria criteria1 = new Criteria();
            criteria1.and("id").is(value.getId());

            Query query1 = new Query(criteria1);

            Update update = new Update();
            update.set("dynamicSegments", value.getDynamicSegments());
            update.set("dynamicSegmentsLastDate", new Date());

            mongoOperations.updateFirst(query1, update, Customer.class);
        }
    }

    private void processDynamicSegmentNumber(MongoOperations mongoOperations, BiqaPaginationResultList list, DynamicSegment dynamicSegment) {
        Criteria criteria1 = new Criteria();
        criteria1.and("id").is(dynamicSegment.getId());

        Query query1 = new Query(criteria1);

        Update update = new Update();
        update.set("lastUpdate", new Date());
        update.set("lastUpdateNumber", list.getEntityNumber());

        mongoOperations.updateFirst(query1, update, DynamicSegment.class);
    }

    private void setDynamicSegmentAsNonProcessed(MongoOperations mongoOperations, DynamicSegment dynamicSegment) {
        Criteria criteria1 = new Criteria();
        criteria1.and("id").is(dynamicSegment.getId());

        Query query1 = new Query(criteria1);

        Update update = new Update();
        update.set("lastUpdate", null);

        mongoOperations.updateFirst(query1, update, DynamicSegment.class);
    }

    private void processDomainDynamicSegment(String domain) {
        MongoOperations mongoOperations = mongoTenantHelper.domainDataBaseUnsafeGet(domain);
        Map<String, Customer> customerMap = new HashMap<>();

        Criteria criteria = new Criteria();
        Query query = new Query(criteria);

        List<DynamicSegment> dynamicSegments = mongoOperations.find(query, DynamicSegment.class);

        for (DynamicSegment dynamicSegment : dynamicSegments) {
            logger.info("Process dynamic segment Id {} , domain {}", dynamicSegment.getId(), domain);
            CustomerFilter customerBuilder = dynamicSegment.getCustomerBuilder();

            if (customerBuilder.isShowOnlyWhenIamResponsible()) {
                logger.info("Skip customerBuilder with 'showOnlyWhenIamResponsible=true'");
                setDynamicSegmentAsNonProcessed(mongoOperations, dynamicSegment);
                continue;
            }

            List<String> ids = new ArrayList<>();
            ids.add("id");
            ids.add("_id");

            customerBuilder.setUseFieldsPartly(true);
            customerBuilder.setPartlyFields(ids);

            BiqaPaginationResultList<Customer> list = customerFilterService.getCustomersByFilterForDomain(customerBuilder, mongoOperations);

            List<Customer> customers = list.getResultedObjects();

            for (Customer customer : customers) {
                processCustomer(customerMap, customer, dynamicSegment);
            }

            processDynamicSegmentNumber(mongoOperations, list, dynamicSegment);
        }
        processCustomerAndUpdate(customerMap, mongoOperations);
    }

    /**
     * Process dynamic segments for customers
     */
    public void createAllDynamicSegmentsCustomers() {
        List<Domain> domains = microserviceDomain.unsafeFindAllDomains();

        for (Domain domainInCRM : domains) {
            String domain = domainInCRM.getDomain();

            if (!domainInCRM.isActive()) {
                logger.info("Skipping inactive domain: " + domain);
                continue;
            }

            service.submit(() -> {
                try {
                    processDomainDynamicSegment(domain);
                } catch (Exception e) {
                    logger.error("processDomainDynamicSegment domain" + domain, e);
                }
            });
        }
    }

}
