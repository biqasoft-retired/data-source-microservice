/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.datasource.controllers;

import com.biqasoft.datasourcebatching.datasource.DataSourceControllerAbstract;
import com.biqasoft.common.exceptions.ThrowExceptionHelper;
import com.biqasoft.entity.datasources.DataSource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;
import com.biqasoft.entity.constants.DATA_SOURCES;
import com.biqasoft.entity.constants.DATA_SOURCES_RETURNED_TYPES;
import com.biqasoft.entity.core.objects.field.DataSourcesTypes;
import com.biqasoft.datasourcebatching.services.KPIService;

/**
 * Created by Nikita Bakaev, ya@nbakaev.ru on 1/16/2016.
 * All Rights Reserved
 */
@Service
public class DataSourceInsideValueController extends DataSourceControllerAbstract {

    public DataSourceInsideValueController() {
        this.name = DATA_SOURCES.INSIDE_VALUE;
    }

    @Override
    public void process(DataSource data) {
        checkNormal(data);
        data.setValues(getInsideValue(data));
    }

    @Override
    public void processWithMongoTemplate(DataSource data, MongoOperations template) {
        this.process(data);
    }

    private void checkNormal (DataSource data){
        if (data.getReturnType() == null || data.getReturnType().equals("")) {
            ThrowExceptionHelper.throwExceptionInvalidRequest("ERROR RESOLVING. INSIDE_VALUE MUST HAVE RETURN TYPE dataSource Id: " + data.getId());
        }
    }

    private DataSourcesTypes getInsideValue(DataSource data) {
        DataSourcesTypes types = new DataSourcesTypes();
        String value = KPIService.getParameterValue("value", data.getParameters());
        if (value == null){
            data.setResolved(false);
            return types;
        }

        if (data.getReturnType().equals(DATA_SOURCES_RETURNED_TYPES.STRING)) {
            types.setStringVal(value);
            data.setResolved(true);
        } else if (data.getReturnType().equals(DATA_SOURCES_RETURNED_TYPES.INTEGER)) {
            types.setIntVal(Integer.parseInt(value));
            data.setResolved(true);
        }
        return types;
    }

}
