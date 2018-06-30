package com.biqasoft.datasourcebatching.batching;

import com.biqasoft.entity.payments.Payment;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import com.biqasoft.entity.constants.CUSTOMER_FIELDS;
import com.biqasoft.entity.customer.Customer;
import com.biqasoft.gateway.indicators.dto.DealStats;
import com.biqasoft.gateway.indicators.dto.IndicatorsDTO;
import com.biqasoft.entity.payments.CompanyCost;
import com.biqasoft.entity.payments.CustomerDeal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@Service
public class LeadGenProjectKPIsRepository {

    public BigDecimal getCostsAmountByList(List<CompanyCost> customerDeals) {
        return customerDeals.parallelStream().map(Payment::getAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public BigDecimal getDealsAmountByList(List<CustomerDeal> customerDeals) {
        return customerDeals.parallelStream().map(Payment::getAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public IndicatorsDTO getIndicatorsForLeadGenProject(String leadGenProjectId, MongoOperations mongoOperations) {
        IndicatorsDTO indicatorsDAO = new IndicatorsDTO();

        Criteria criteria = new Criteria();
        criteria.and("leadGenProjectId").is(leadGenProjectId);
        Query query = new Query(criteria);
        List<CustomerDeal> dealsList = mongoOperations.find(query, CustomerDeal.class);

        Criteria criteria2 = new Criteria();
        criteria2.and("leadGenProjectId").is(leadGenProjectId);
        Query query2 = new Query(criteria2);
        List<CompanyCost> companyCosts = mongoOperations.find(query2, CompanyCost.class);

        indicatorsDAO.setCostsAmount(this.getCostsAmountByList(companyCosts));
        indicatorsDAO.setDealsAmounts(this.getDealsAmountByList(dealsList));

        indicatorsDAO.setDealsNumber(dealsList.size());
        indicatorsDAO.setCostsNumber(companyCosts.size());

        List<DealStats> dealStatses = new ArrayList<>();
        List<CustomerDeal> deals = dealsList;

        if (deals.size() > 0) {
            for (CustomerDeal customerDeal : deals) {
                DealStats stats = new DealStats();
                stats.setCustomerDealAmount(customerDeal.getAmount());
                stats.setResponsibleManagerID(customerDeal.getCreatedInfo().getCreatedById());
                stats.setCustomerDealCycle(customerDeal.getDealsCycle());

                dealStatses.add(stats);
            }

            Double a = deals.stream().mapToLong(x -> x.getDealsCycle()).average().getAsDouble();
            indicatorsDAO.setDealsCycle(new BigDecimal(a.toString()));
            indicatorsDAO.setAveragePayment(averagePaymentByLeadGenProject(indicatorsDAO, dealsList));
            indicatorsDAO.setROI(getROIByLeadGenProjectId(indicatorsDAO, companyCosts));
        }

        indicatorsDAO.setCustomersNumber(getCustomersNumberByLeadGenProject(leadGenProjectId, mongoOperations));
        indicatorsDAO.setLeadsNumber(getLeadsNumberByLeadGenMethodAndProject(leadGenProjectId, mongoOperations));

        if (deals.size() > 0) {
            indicatorsDAO.setLtv(getCustomerLifeTimeValueByLeadGenProject(indicatorsDAO));
        }

        indicatorsDAO.setLeadCost(leadCostByLeadGenMethod(indicatorsDAO));
        indicatorsDAO.setCustomerCost(customerCost(indicatorsDAO));
        indicatorsDAO.setConversionFromLeadToCustomer(conversionFromLeadToCustomerByLeadGenMethodAndProject(indicatorsDAO));

        indicatorsDAO.setIndicatorID(leadGenProjectId);
        indicatorsDAO.setType(CUSTOMER_FIELDS.LEAD_GEN_PROJECT);

        return indicatorsDAO;
    }

    private BigDecimal getROIByLeadGenProjectId(IndicatorsDTO indicatorsDAO, List<CompanyCost> costs) {
        if (costs.size() == 0)  return new BigDecimal(BigInteger.ZERO);
        return indicatorsDAO.getDealsAmounts().divide( indicatorsDAO.getCostsAmount() , RoundingMode.HALF_UP );
    }

    // customer number by lead gen project
    private long getCustomersNumberByLeadGenProject(String projectId, MongoOperations mongoOperations) {
        Criteria criteria = new Criteria();
        criteria.and("leadGenProject").is(projectId).and("customer").is(true);

        Query query = new Query(criteria);
        return mongoOperations.count(query, Customer.class);
    }

    // leads number by lead gen project
    private long getLeadsNumberByLeadGenMethodAndProject(String projectId, MongoOperations mongoOperations) {
        Criteria criteria = new Criteria();
        criteria.and("leadGenProject").is(projectId).and("lead").is(true);

        Query query = new Query(criteria);
        return mongoOperations.count(query, Customer.class);
    }

    private BigDecimal conversionFromLeadToCustomerByLeadGenMethodAndProject(IndicatorsDTO indicatorsDAO) {
        if (indicatorsDAO.getCustomersNumber() == 0) return new BigDecimal(BigInteger.ZERO);

        Long leadsPlusCustomerNumber = indicatorsDAO.getLeadsNumber() + indicatorsDAO.getCustomersNumber();

        BigDecimal result = new BigDecimal(indicatorsDAO.getCustomersNumber()).divide(new BigDecimal(leadsPlusCustomerNumber.toString()) , RoundingMode.HALF_UP );
        return result;
    }

    private BigDecimal leadCostByLeadGenMethod(IndicatorsDTO indicatorsDAO) {
        if ((indicatorsDAO.getLeadsNumber() + indicatorsDAO.getCustomersNumber()) == 0) return new BigDecimal(BigInteger.ZERO);

        Long leadsPlusCustomerNumber = indicatorsDAO.getLeadsNumber() + indicatorsDAO.getCustomersNumber();

        return indicatorsDAO.getCostsAmount().divide(new BigDecimal(leadsPlusCustomerNumber.toString()), RoundingMode.HALF_UP);
    }

    private BigDecimal averagePaymentByLeadGenProject(IndicatorsDTO indicatorsDAO, List<CustomerDeal> deals) {

        if (deals.size() == 0) return new BigDecimal(BigInteger.ZERO);
        return indicatorsDAO.getDealsAmounts().divide(new BigDecimal(Integer.valueOf(deals.size()).toString()) , RoundingMode.HALF_UP);
    }

    private BigDecimal getCustomerLifeTimeValueByLeadGenProject(IndicatorsDTO indicatorsDAO) {
        return indicatorsDAO.getDealsAmounts().divide(new BigDecimal(Long.valueOf(indicatorsDAO.getCustomersNumber()).toString()) , RoundingMode.HALF_UP);
    }

    private BigDecimal customerCost(IndicatorsDTO indicatorsDAO) {
        if (indicatorsDAO.getCustomersNumber() == 0 || indicatorsDAO.getCostsAmount().equals(new BigDecimal(BigInteger.ZERO)))
            return new BigDecimal(BigInteger.ZERO);
        return (indicatorsDAO.getCostsAmount().divide(new BigDecimal(Long.valueOf(indicatorsDAO.getCustomersNumber()).toString()) , RoundingMode.HALF_UP));
    }

}
