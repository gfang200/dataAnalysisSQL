SELECT *
FROM ( $base_dates cal
      LEFT JOIN
        (SELECT partition_date,
                count(opt_out_id) AS total_opt_out
         FROM
           (SELECT *
            FROM
              (SELECT base.opt_out_id ,
                      chirp_email_prefs /*, case when link_date between '2016-11-04' and '2016-11-09' then link_date
         when link_date between '2016-11-12' and '2016-11-13' then link_date
          else partition_date end as partition_date*/ ,
                      partition_date ,
                      previous_email_prefs ,
                      CASE WHEN previous_email_prefs IS TRUE
               AND chirp_email_prefs IS FALSE THEN 1 ELSE 0 END AS optout
               FROM
                 ( SELECT opt_out_id ,
                          chirp_email_prefs ,
                          link_date ,
                          partition_date ,
                          lag(chirp_email_prefs) over (partition BY opt_out_id
                                                       ORDER BY partition_date) AS previous_email_prefs
                  FROM
                    ( SELECT opt_out_id ,
                             chirp_email_prefs ,
                             date(cast(substr(cast(a._partition_date AS string),0,4) AS int64), cast(substr(cast(a._partition_date AS string),5,2) AS int64) , cast(substr(cast(a._partition_date AS string),7,2) AS int64)) AS partition_date ,
date(TIMESTAMP_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) AS link_date
                     FROM lrs.history.email_preferences.all a,
                          unnest(a.device_data) b
                     JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter --  and cast(a._partition_date as int64) between 20161110 and 20161129

                     GROUP BY 1,
                              2,
                              3,
                              4)) base)
WHERE optout=1
AND opt_out_id IN
(SELECT opt_out_id
FROM lrs.history.email_preferences.yesterday
WHERE chirp_email_prefs = FALSE
GROUP BY 1))
         GROUP BY 1 ) total_opt_out ON total_opt_out.partition_date = cal.cal_date
      LEFT JOIN
( SELECT partition_date,
 count(opt_out_id) AS email_opt_out
FROM
(SELECT *
FROM
(SELECT base.opt_out_id ,
    chirp_email_prefs /*, case when link_date between '2016-11-04' and '2016-11-09' then link_date
         when link_date between '2016-11-12' and '2016-11-13' then link_date
          else partition_date end as partition_date*/ ,
    partition_date ,
    previous_email_prefs ,
    CASE WHEN previous_email_prefs IS TRUE
AND chirp_email_prefs IS FALSE THEN 1 ELSE 0 END AS optout
FROM
( SELECT opt_out_id ,
       chirp_email_prefs ,
       link_date ,
       partition_date ,
       lag(chirp_email_prefs) over (partition BY opt_out_id
                                  ORDER BY partition_date) AS previous_email_prefs
FROM
 ( SELECT opt_out_id ,
          chirp_email_prefs ,
          date(cast(substr(cast(a._partition_date AS string),0,4) AS int64), cast(substr(cast(a._partition_date AS string),5,2) AS int64) , cast(substr(cast(a._partition_date AS string),7,2) AS int64)) AS partition_date ,
date(TIMESTAMP_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) AS link_date
  FROM lrs.history.email_preferences.all a,
       unnest(a.device_data) b
  JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter --  and cast(a._partition_date as int64) between 20161110 and 20161129

  GROUP BY 1,
           2,
           3,
           4)) base)
WHERE optout=1
AND opt_out_id IN
(SELECT opt_out_id
FROM lrs.history.email_preferences.yesterday
WHERE chirp_email_prefs = FALSE
GROUP BY 1)
AND opt_out_id IN
(SELECT signal_1
FROM lrs.healthcheck_OptOutMessages a
JOIN lrs.healthcheck_SentMessages b ON a.sent_message_id = b.sent_message_id))
GROUP BY 1 ) email_opt ON email_opt.partition_date = cal.cal_date
      LEFT JOIN
( SELECT partition_date,
 count(opt_out_id) AS onboarding_opt_out
FROM
(SELECT *
FROM
(SELECT base.opt_out_id ,
    chirp_email_prefs /*, case when link_date between '2016-11-04' and '2016-11-09' then link_date
         when link_date between '2016-11-12' and '2016-11-13' then link_date
          else partition_date end as partition_date*/ ,
    partition_date ,
    previous_email_prefs ,
    CASE WHEN previous_email_prefs IS TRUE
AND chirp_email_prefs IS FALSE THEN 1 ELSE 0 END AS optout
FROM
( SELECT opt_out_id ,
       chirp_email_prefs ,
       link_date ,
       partition_date ,
       lag(chirp_email_prefs) over (partition BY opt_out_id
                                  ORDER BY partition_date) AS previous_email_prefs
FROM
 ( SELECT opt_out_id ,
          chirp_email_prefs ,
          date(cast(substr(cast(a._partition_date AS string),0,4) AS int64), cast(substr(cast(a._partition_date AS string),5,2) AS int64) , cast(substr(cast(a._partition_date AS string),7,2) AS int64)) AS partition_date ,
date(TIMESTAMP_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) AS link_date
  FROM lrs.history.email_preferences.all a,
       unnest(a.device_data) b
  JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter --  and cast(a._partition_date as int64) between 20161110 and 20161129

  GROUP BY 1,
           2,
           3,
           4)) base)
WHERE optout=1
AND opt_out_id IN
(SELECT opt_out_id
FROM lrs.history.email_preferences.yesterday
WHERE chirp_email_prefs = FALSE
GROUP BY 1)
AND opt_out_id IN
(SELECT signal_1
FROM lrs.healthcheck_OptOutMessages a
JOIN lrs.healthcheck_SentMessages b ON a.sent_message_id = b.sent_message_id)
AND opt_out_id IN
(SELECT signal_1
FROM $id_email(OptOutMessages)
WHERE a.campaign_id IN ($onboarding_ids)))
GROUP BY 1 ) onboarding_opt ON onboarding_opt.partition_date = cal.cal_date
      LEFT JOIN
( SELECT current_sub_date,
 CASE WHEN current_sub_date ='2016-11-10' THEN subscribers ELSE subscribers - lag(subscribers) over (
                                                                                             ORDER BY current_sub_date) END subscribers
FROM
( SELECT partition_date AS current_sub_date ,
                     count(opt_out_id) subscribers
FROM
( SELECT opt_out_id ,
     chirp_email_prefs ,
     partition_date ,
     previous_email_prefs
 /*
    , case when previous_email_prefs is null and chirp_email_prefs = true then 1
            when previous_email_prefs = false and chirp_email_prefs = true then 1
            else  0 end as optin
    , case when previous_email_prefs is null and chirp_email_prefs = false then 1
            when previous_email_prefs is false and chirp_email_prefs = true then 1
            else  0 end as optout */
FROM
(SELECT opt_out_id ,
      chirp_email_prefs ,
      link_date ,
      partition_date ,
      lag(chirp_email_prefs) over (partition BY opt_out_id
                                 ORDER BY partition_date) AS previous_email_prefs
FROM
 ( SELECT opt_out_id ,
          chirp_email_prefs ,
          date(cast(substr(cast(a._partition_date AS string),0,4) AS int64), cast(substr(cast(a._partition_date AS string),5,2) AS int64) , cast(substr(cast(a._partition_date AS string),7,2) AS int64)) AS partition_date ,
date(TIMESTAMP_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) AS link_date
  FROM lrs.history.email_preferences.all a,
       unnest(a.device_data) b
  JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter --  and cast(a._partition_date as int64) between 20161110 and 20161129

  GROUP BY 1,
           2,
           3,
           4)))
WHERE chirp_email_prefs = TRUE
GROUP BY 1 )) b ON b.current_sub_date = cal.cal_date)
