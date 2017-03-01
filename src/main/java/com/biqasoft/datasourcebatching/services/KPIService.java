/*
* Copyright (c) 2016 biqasoft.com




 */

package com.biqasoft.datasourcebatching.services;

import com.biqasoft.datasourcebatching.datasource.DataSourceResolving;
import com.biqasoft.entity.constants.DATA_SOURCES_MORE;
import com.biqasoft.entity.constants.DATA_SOURCES_RETURNED_TYPES;
import com.biqasoft.entity.core.CreatedInfo;
import com.biqasoft.entity.core.Domain;
import com.biqasoft.entity.core.objects.field.DataSourcesTypes;
import com.biqasoft.entity.datasources.DataSource;
import com.biqasoft.entity.datasources.SavedDataSource;
import com.biqasoft.entity.system.NameValueMap;
import com.biqasoft.microservice.common.MicroserviceDomain;
import com.biqasoft.microservice.database.MongoTenantHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KPIService {

    private static final Logger logger = LoggerFactory.getLogger(KPIService.class);
    private final ExecutorService service = Executors.newCachedThreadPool();

    private final DataSourceResolving dataSourceResolving;
    private final MongoTenantHelper mongoTenantHelper;
    private final MicroserviceDomain microserviceDomain;

    @Autowired
    public KPIService(MongoTenantHelper mongoTenantHelper, MicroserviceDomain microserviceDomain, DataSourceResolving dataSourceResolving) {
        this.mongoTenantHelper = mongoTenantHelper;
        this.microserviceDomain = microserviceDomain;
        this.dataSourceResolving = dataSourceResolving;
    }

    /**
     * parse params to controller
     *
     * @param param
     * @param params
     * @return
     */
    public static String getParameterValue(String param, List<NameValueMap> params) {
        for (NameValueMap nameValueMap : params) {
            if (nameValueMap.getName().equals(param)) {
                return nameValueMap.getValue();
            }
        }
        return null;
    }

    /**
     * basic check is data source MINIMUM (every required fields for ALL dataSourcesControllers are set) valid - have controller etc...
     *
     * @param dataSource
     * @return
     */
    public static boolean isNormalToController(DataSource dataSource) {
        return !(dataSource == null || dataSource.getControlledClass() == null || dataSource.getControlledClass().equals(""));
    }

    /**
     * This method
     * 1) set correct DataSourcesTypes for DataSource.values
     * 2) set DataSource.resolved to true
     *
     * @param data
     * @param template
     * @return
     * @throws Exception
     */
    private DataSourcesTypes getValue(DataSource data, MongoOperations template) throws Exception {
        if (!isNormalToController(data)) {
            throw new Exception("ERROR RESOLVING. NOT NORMAL");
        }

        DataSourcesTypes types;

        try {
            dataSourceResolving.getDataSourceProcessed(data, template);
            types = data.getValues();
        } catch (Exception e) {
            String message = e.getMessage();
            if (message == null || message.length() == 0) {
                message = "Error processing data source with id: " + data.getId();
            }
            data.setErrorResolvedMessage(message);
            throw new RuntimeException(message);
        }

        return types;
    }

    /**
     * update dataSource to mongoDB database
     *
     * @param abstractDataSource
     * @param types
     * @param template
     */
    private void updateToDataBase(DataSource abstractDataSource, DataSourcesTypes types, MongoOperations template) {
        abstractDataSource.setValues(types);
        abstractDataSource.setLastUpdate(new Date());
        template.save(abstractDataSource);
    }

    /**
     * save data source metric with data
     *
     * @param types
     * @param abstractDataSource
     * @param template
     */
    private void saveDataSourceValuesToDataBase(DataSourcesTypes types, DataSource abstractDataSource, MongoOperations template) {
        SavedDataSource dat = new SavedDataSource();
        dat.setCreatedInfo(new CreatedInfo(new Date()));
        dat.setValues(types);
        dat.setDataSourceId(abstractDataSource.getId());

        template.save(dat);
    }

    private void checkAndSetLights(DataSource dataSource) {
        if (dataSource == null) return;

        try {
            if (!dataSource.getReturnType().equals(DATA_SOURCES_RETURNED_TYPES.INTEGER)) return;

            Integer success = dataSource.getLights().getSuccess().getValue().getIntVal();
            Integer warning = dataSource.getLights().getWarning().getValue().getIntVal();
            Integer error = dataSource.getLights().getError().getValue().getIntVal();

            Integer currentValue = dataSource.getValues().getIntVal();

            if (success == null || warning == null || error == null){
                return;
            }

            if (currentValue >= success) {
                dataSource.getLights().setCurrentLight(DATA_SOURCES_MORE.SUCCESS);
                return;
            }
            if (currentValue < success && (currentValue >= warning || currentValue > error)) {
                dataSource.getLights().setCurrentLight(DATA_SOURCES_MORE.WARNING);
                return;
            }
            if (currentValue <= error) {
                dataSource.getLights().setCurrentLight(DATA_SOURCES_MORE.ERROR);
                return;
            }

        } catch (Exception e) {
        }

    }

    /**
     * save new dataSource with the latest value
     * and save this metric in timeline
     *
     * @param abstractDataSource
     * @param template
     * @return
     */
    public DataSource processDataSource(DataSource abstractDataSource, MongoOperations template) {
        try {

            // we expect that `getValue()` will set `resolved` field to true`
            DataSourcesTypes types = getValue(abstractDataSource, template);

            if (!abstractDataSource.isResolved()) {
                throw new RuntimeException("ERROR RESOLVING Id: " + abstractDataSource.getId());
            } else {
                checkAndSetLights(abstractDataSource);
                saveDataSourceValuesToDataBase(types, abstractDataSource, template);
                updateToDataBase(abstractDataSource, types, template);
            }
        } catch (Exception e) {

            abstractDataSource.setResolved(false);
            abstractDataSource.setLastUpdate(new Date());
            template.save(abstractDataSource);

            logger.warn(e.getMessage());
        }
        return abstractDataSource;
    }

    public void processDomain(Domain domain){
        MongoOperations template = mongoTenantHelper.domainDataBaseUnsafeGet(domain.getDomain());
        Criteria criteria = new Criteria();
        Query query = new Query(criteria);

        // get all data source meta info
        List<DataSource> datas = template.find(query, DataSource.class);

        for (DataSource abstractDataSource : datas) {
            // async execute
            service.submit(() -> {
                try {
                    processDataSource(abstractDataSource, template);
                } catch (Exception e) {
                    logger.error("process kpi domain {} ds {}", domain.getDomain(), abstractDataSource.getId(), e);
                }
            });
        }
    }

    public void processAllDomains() {
        List<Domain> domains = microserviceDomain.unsafeFindAllDomains();

        for (Domain domain : domains) {
            // skip not active domains
            if (!domain.isActive()){
                continue;
            }

            processDomain(domain);
        }
    }

}
