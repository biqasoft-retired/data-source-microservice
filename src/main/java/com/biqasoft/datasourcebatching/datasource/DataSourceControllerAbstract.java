/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.datasource;

import com.biqasoft.entity.datasources.DataSource;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * Created by Nikita Bakaev, ya@nbakaev.ru on 1/16/2016.
 * All Rights Reserved
 */
public abstract class DataSourceControllerAbstract {

    protected String name = null;
    protected String returnType = null;

    /**
     * Default every controller should set `true` if all is OK at the resolving END
     * @param data
     * @return
     */
    public boolean isResolved (DataSource data){
        if (data.isResolved()) return true;
        return false;
    }

    public abstract void process (DataSource data);
    public abstract void processWithMongoTemplate (DataSource data, MongoOperations template);

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
