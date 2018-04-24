SELECT rev.universal_account_id,
       cids.acquisition_channel,
       cids.activationdateid,
       acquisition_subchannel,
       country.region,
       cast(period_idtoid(cids.activationdateid, "DATEID", "QUARTEROFYEAR") AS int32) AS activation_quarter,
       CASE
           WHEN cids.activationdateid + 91 < 6611 THEN SUM(CASE WHEN rev.date_id < cids.activationdateid + 91 THEN rev.cost_usd_quarterly_fx ELSE 0.0 END)
           ELSE SUM(CASE WHEN rev.date_id < cids.activationdateid + 91 THEN rev.cost_usd_quarterly_fx ELSE 0.0 END) * (91/(6611 - cids.activationdateid))
       END AS gross_revenue_91d ,
       CASE
           WHEN cids.activationdateid + 91 < 6611 THEN 0
           ELSE 1
       END AS adjusted,
       CASE
           WHEN SUM(CASE WHEN rev.date_id < cids.activationdateid + 91 THEN rev.cost_usd_quarterly_fx ELSE 0.0 END) >= 75000 THEN "Top"
           WHEN SUM(CASE WHEN rev.date_id < cids.activationdateid + 91 THEN rev.cost_usd_quarterly_fx ELSE 0.0 END) >= 30000 THEN "Mid"
           WHEN SUM(CASE WHEN rev.date_id < cids.activationdateid + 91 THEN rev.cost_usd_quarterly_fx ELSE 0.0 END) >= 5000 THEN "Bottom"
           ELSE "Longtail"
       END AS tier
FROM bimkt_prd_cid_lifecycle_stats_product rev
INNER JOIN bimkt_prd_acquisition_cids AS cids ON (rev.universal_account_id = cids.universal_account_id
                                                  AND rev.date_id >= cids.activationdateid
                                                  AND rev.date_id <= ifnull(cids.next_event_date_id - 1, 99999))
LEFT OUTER JOIN bigad_country_mapping country ON country.country_code2 = cids.acquisition_country_code -- MMR filters

WHERE cids.billing_category = 'Billable'
  AND cids.service_country_code <> 'UN'
  AND cids.fraudster = FALSE -- Other filters

  AND period_idtoid(cids.activationdateid,"DATEID","YEARID") = 2017
GROUP BY 1,
         2,
         3,
         4,
         5,
         6
