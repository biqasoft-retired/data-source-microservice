/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.datasource.controllers;

import com.biqasoft.common.exceptions.ThrowExceptionHelper;
import com.biqasoft.customer.CustomerFilterService;
import com.biqasoft.datasourcebatching.datasource.DataSourceControllerAbstract;
import com.biqasoft.datasourcebatching.services.KPIService;
import com.biqasoft.entity.constants.DATA_SOURCES;
import com.biqasoft.entity.constants.DATA_SOURCES_RETURNED_TYPES;
import com.biqasoft.entity.core.objects.field.DataSourcesTypes;
import com.biqasoft.entity.customer.DynamicSegment;
import com.biqasoft.entity.datasources.DataSource;
import com.biqasoft.entity.filters.CustomerFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Created by Nikita Bakaev, ya@nbakaev.ru on 1/16/2016.
 * All Rights Reserved
 */
@Service
public class CustomersByDynamicSegmentController extends DataSourceControllerAbstract {

    private final CustomerFilterService customerFilterService;

    @Autowired
    public CustomersByDynamicSegmentController(CustomerFilterService customerFilterService) {
        this.name = DATA_SOURCES.CUSTOMERS_BY_DYNAMIC_SEGMENT_ID;
        this.setReturnType(DATA_SOURCES_RETURNED_TYPES.INTEGER);
        this.customerFilterService = customerFilterService;
    }

    @Override
    public void process(DataSource data) {
    }

    @Override
    public void processWithMongoTemplate(DataSource data, MongoOperations template) {
        data.setValues(getCustomerBySalesFunnelId(data, template));
        data.setReturnType(this.returnType);
    }

    private DataSourcesTypes getCustomerBySalesFunnelId(DataSource dataSource, MongoOperations template) {
        String salesFunnelStatusName = KPIService.getParameterValue("dynamicSegmentId", dataSource.getParameters());

        if (StringUtils.isEmpty(salesFunnelStatusName)) {
            ThrowExceptionHelper.throwExceptionInvalidRequest("YOU SHOULD GIVE dynamicSegmentId ('dynamicSegmentId') IN PARAMS");
        }

        DynamicSegment s = findDynamicSegmentById(salesFunnelStatusName, template);

        if (s == null || s.getCustomerBuilder() == null) {
            ThrowExceptionHelper.throwExceptionInvalidRequest("No such dynamic segment: " + salesFunnelStatusName);
        }

        CustomerFilter customerBuilder = s.getCustomerBuilder();
        customerBuilder.setOnlyCount(true);

        DataSourcesTypes types = new DataSourcesTypes();
        types.setIntVal((int) customerFilterService.getCustomersByFilterForDomain(customerBuilder, template).getEntityNumber());

        if (types.getIntVal() != null && types.getIntVal() >= 0) {
            dataSource.setResolved(true);
        }
        return types;
    }

    private DynamicSegment findDynamicSegmentById(String dynamicSegmentId, MongoOperations ops) {
        return ops.findOne(Query.query(Criteria.where("id").is(dynamicSegmentId)), DynamicSegment.class);
    }

}
