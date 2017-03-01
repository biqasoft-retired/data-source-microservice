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
import com.biqasoft.entity.core.RegexBuilderNode;
import com.biqasoft.entity.core.objects.field.DataSourcesTypes;
import com.biqasoft.entity.datasources.DataSource;
import com.biqasoft.entity.filters.CustomerFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nikita Bakaev, ya@nbakaev.ru on 1/16/2016.
 * All Rights Reserved
 */
@Service
public class DataSourceCustomersBySalesFunnelNameController extends DataSourceControllerAbstract {

    private final CustomerFilterService customerFilterService;

    @Autowired
    public DataSourceCustomersBySalesFunnelNameController(CustomerFilterService customerFilterService) {
        this.name = DATA_SOURCES.CUSTOMERS_OR_LEADS_NUMBER_BY_SALES_FUNNEL_STATUS_NAME;
        this.setReturnType(DATA_SOURCES_RETURNED_TYPES.INTEGER);
        this.customerFilterService = customerFilterService;
    }

    @Override
    public void process(DataSource data) {}

    @Override
    public void processWithMongoTemplate(DataSource data, MongoOperations template) {
        data.setValues(getCustomerBySalesFunnelId(data, template));
        data.setReturnType(this.returnType);
    }

    private DataSourcesTypes getCustomerBySalesFunnelId(DataSource dataSource, MongoOperations template) {
        String salesFunnelStatusName = KPIService.getParameterValue("salesFunnelStatusName", dataSource.getParameters());

        if (salesFunnelStatusName == null) {
            ThrowExceptionHelper.throwExceptionInvalidRequest("YOU SHOULD GIVE SALES FUNNEL STATUS NAME ('salesFunnelStatusName') IN PARAMS");
        }

        CustomerFilter customerBuilder = new CustomerFilter();
        customerBuilder.setOnlyCount(true);
        customerBuilder.setUseRegex(true);

        List<RegexBuilderNode> nodes = new ArrayList<>();
        RegexBuilderNode node = new RegexBuilderNode();
        node.setFieldName("salesFunnelStatus.name");
        node.setValue(salesFunnelStatusName);

        nodes.add(node);

        customerBuilder.setRegexBuilderNodes(nodes);

        DataSourcesTypes types = new DataSourcesTypes();
        types.setIntVal((int) customerFilterService.getCustomersByFilterForDomain(customerBuilder, template).getEntityNumber());

        if (types.getIntVal() != null && types.getIntVal() >= 0) {
            dataSource.setResolved(true);
        }
        return types;
    }

}
