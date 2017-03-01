/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.datasource.controllers;

import com.biqasoft.bpmn.safejs.ExecutorCodeService;
import com.biqasoft.bpmn.safejs.entity.ExecuteJsRequest;
import com.biqasoft.bpmn.safejs.entity.ExecuteJsResponse;
import com.biqasoft.entity.datasources.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;
import com.biqasoft.datasourcebatching.datasource.DataSourceControllerAbstract;
import com.biqasoft.entity.constants.DATA_SOURCES;
import com.biqasoft.entity.core.objects.field.DataSourcesTypes;
import com.biqasoft.datasourcebatching.services.KPIService;

import java.util.Date;

/**
 * Created by Nikita Bakaev, ya@nbakaev.ru on 1/16/2016.
 * All Rights Reserved
 */
@Service
public class SafeJsCodeExecuteController extends DataSourceControllerAbstract {

    public SafeJsCodeExecuteController() {
        this.name = DATA_SOURCES.SAFE_JS_CODE_EXECUTE;
    }

    @Autowired
    private ExecutorCodeService executorCodeService;

    @Override
    public void process(DataSource data) {
    }

    @Override
    public void processWithMongoTemplate(DataSource data, MongoOperations template) {
        data.setValues(executeCode(data, template));
        data.setResolved(true);
//        data.setReturnType(this.returnType);
    }

    private DataSourcesTypes executeCode(DataSource data, MongoOperations template) {
        DataSourcesTypes types = new DataSourcesTypes();

        String jsCode = KPIService.getParameterValue("jsCode", data.getParameters());

        ExecuteJsRequest executeJsRequest = new ExecuteJsRequest();

        executeJsRequest.setJsCode(jsCode);
        ExecuteJsResponse executeJsResponse = executorCodeService.executeCode(executeJsRequest);

        Object result = executeJsResponse.getResult();

        if ( result instanceof String){
            types.setStringVal((String) result);
        }else if (result instanceof Integer){
            types.setIntVal((Integer) result);
        }else if (result instanceof Double){
            types.setDoubleVal((Double) result);
        }else if (result instanceof Date){
            types.setDateVal((Date) result);
        }
        return types;
    }

}
