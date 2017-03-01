### This module save KPIs to dataBase (mongoDB) to view data if history later

Save leadGenMethod and leadGenProject KPIs: GET `http://localhost:9090/main/ds`
Save dataSource KPIs                      : GET `http://localhost:9090/main/data-source-save`
```
no response body
```

GET ALL SUPPORTED dataSourceControllers     GET `http://localhost:9090/main/data-source-save/supported-controllers`
```json
{"INSIDE_VALUE":{"name":"INSIDE_VALUE","returnType":null},"CUSTOMERS_OR_LEADS_NUMBER_BY_SALES_FUNNEL_STATUS":{"name":"CUSTOMERS_OR_LEADS_NUMBER_BY_SALES_FUNNEL_STATUS","returnType":"INTEGER"}}
```

Save DynamicSegments for every customer    : GET `http://localhost:9090/main/customers/dynamic-segment`

### Notes
some methods (2) have `@Scheduled` annotation to work as cron