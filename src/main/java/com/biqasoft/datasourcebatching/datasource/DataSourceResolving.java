/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.datasource;

import com.biqasoft.common.exceptions.ThrowExceptionHelper;
import com.biqasoft.entity.datasources.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * This is main class which Resolving datasources
 *
 * Created by Nikita Bakaev, ya@nbakaev.ru on 1/16/2016.
 * All Rights Reserved
 */
@Service
public class DataSourceResolving {

    // here we have references to Objects(Spring common) which resolves datasources
    private Map<String, DataSourceControllerAbstract> classesDataSource = new HashMap<>();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private  <T extends DataSourceControllerAbstract> void putDataSource(T aClass){
        if (aClass == null){
            logger.warn("You try to put NULL dataSourceController");
        }else if (aClass.getName() == null || aClass.getName().length() == 0) {
            logger.warn("Empty dataSourceController name");
        } else{
           if ( classesDataSource.get(aClass.getName()) != null){
               logger.warn("You try to put dataSource controller with name {} which is already exist in processing chain", aClass.getName());
           }
            classesDataSource.put(aClass.getName(), aClass);
            logger.info("Put new DataSourceController with name {} ", aClass.getName());
        }
    }

    @Autowired
    public void registerControllers(List<DataSourceControllerAbstract> controllers){
        controllers.forEach(this::putDataSource);
    }

    public DataSource getDataSourceProcessed(DataSource data, MongoOperations template){
        Object cl =  classesDataSource.get(data.getControlledClass());

        if (cl == null){
            ThrowExceptionHelper.throwExceptionInvalidRequest("NO SUCH CONTROLLER NAME: " + data.getControlledClass());
        }

        DataSourceControllerAbstract c2 = null;
        if (cl != null){
            c2 = (DataSourceControllerAbstract) cl;
        }

        if (c2 == null){
            ThrowExceptionHelper.throwExceptionInvalidRequest("Error processing data source with Id: : " + data.getId());
        }else {
            c2.processWithMongoTemplate(data, template);
        }

        return data;
    }

    public Map<String, DataSourceControllerAbstract> getClassesDataSource() {
        return classesDataSource;
    }

}
