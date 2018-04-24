
SELECT *,
       UNIX_MILLIS(timestamp_add(timestamp(cal_date), interval 8 hour)) AS cal_date_ux,
       FORMAT_DATE('%B',cal_date) AS MONTH
FROM ### Get range of cal dates

  (SELECT date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) cal_date
   FROM lrs.email_preferences a,
        a.device_data b
   JOIN lrs.devices v ON b.device_id = v.device_id
   WHERE $base_filter
   GROUP BY 1) cal
LEFT JOIN
  ( SELECT date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) AS activation_date ,
                                                                                                            count(distinct(b.device_id)) device_activations
   FROM lrs.email_preferences a,
        unnest(a.device_data) b
   JOIN lrs.devices v ON b.device_id = v.device_id
   WHERE $base_filter
   GROUP BY 1) activations ON cal.cal_date = activations.activation_date
LEFT JOIN
  ( SELECT current_sub_date,
           CASE
               WHEN current_sub_date ='2016-11-10' THEN subscribers
               ELSE subscribers - lag(subscribers) over (
                                                         ORDER BY current_sub_date)
           END subscribers
   FROM
     ( SELECT partition_date AS current_sub_date ,
                                count(opt_out_id) subscribers
      FROM
        ( SELECT opt_out_id ,
                 chirp_email_prefs ,
                 partition_date ,
                 previous_email_prefs ####ask why this is here
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
      GROUP BY 1 )) subs ON subs.current_sub_date = cal.cal_date
LEFT JOIN
( SELECT count(distinct(opt_out_id)) current_linked ,
 link_date
FROM
( SELECT a.opt_out_id ,
   chirp_email_prefs ,
   min(date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles"))) link_date
FROM lrs.email_preferences a,
a.device_data b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1,
   2)
GROUP BY 2) current_linked ON cal.cal_date = current_linked.link_date
LEFT JOIN ####Number of emails sent per day

(SELECT date(timestamp_trunc(timestamp_micros(sent_time_usec),DAY,"America/Los_Angeles")) email_send_date ,
count(distinct(sent_message_id)) emails_sent
FROM gamma_reporting.SentMessages.all
WHERE cast(_partition_date AS int64) >= 20161111
AND campaign_id IN ($campaign_ids)
GROUP BY 1) sends ON cal.cal_date = sends.email_send_date
LEFT JOIN ####Number of emails opened per day

(SELECT date(timestamp_trunc(timestamp_micros(open_time_usec),DAY,"America/Los_Angeles")) total_email_open_date ,
count(distinct(sent_message_id)) emails_opened
FROM
(SELECT campaign_id,
  sent_message_id,
  min(open_time_usec) AS open_time_usec
FROM gamma_reporting.Opens.all
WHERE cast(_partition_date AS int64) >= 20161111
AND campaign_id IN ($campaign_ids)
GROUP BY 1,
   2)
GROUP BY 1) total_opens ON cal.cal_date = total_opens.total_email_open_date #################BEGIN EMAIL ANALYTICS#############################################
LEFT JOIN
( SELECT email_send_date AS email_send_date_new,
                    max(CASE WHEN TYPE = 'welcome' THEN emails_sent END) AS welcome_send,
                                                                    max(CASE WHEN TYPE = 'onboarding' THEN emails_sent END) AS onboarding_send,
                                                                                                                       max(CASE WHEN TYPE = 'newsletter' THEN emails_sent END) AS newsletter_send
FROM
(SELECT date(timestamp_trunc(timestamp_micros(sent_time_usec),DAY,"America/Los_Angeles")) email_send_date,
  CASE WHEN campaign_id IN ($onboarding_end) THEN 'onboarding' WHEN campaign_id IN ($welcome_id) THEN 'welcome' WHEN campaign_id IN ($newsletter_ids) THEN 'newsletter' END AS TYPE ,
                                                                                                                                                                         count(distinct(sent_message_id)) emails_sent
FROM gamma_reporting.SentMessages.all
WHERE cast(_partition_date AS int64) >= 20161111
AND campaign_id IN ($campaign_ids)
GROUP BY 1,
   2 )
GROUP BY 1 ) sends2 ON cal.cal_date = sends2.email_send_date_new
LEFT JOIN
( SELECT email_open_date,
 max(CASE WHEN TYPE = 'welcome' THEN emails_sent END) AS welcome_open,
                                                 max(CASE WHEN TYPE = 'onboarding' THEN emails_sent END) AS onboarding_open,
                                                                                                    max(CASE WHEN TYPE = 'newsletter' THEN emails_sent END) AS newsletter_open
FROM
(SELECT date(timestamp_trunc(timestamp_micros(open_time_usec),DAY,"America/Los_Angeles")) email_open_date,
  CASE WHEN campaign_id IN ($onboarding_end) THEN 'onboarding' WHEN campaign_id IN ($welcome_id) THEN 'welcome' WHEN campaign_id IN ($newsletter_ids) THEN 'newsletter' END AS TYPE ,
                                                                                                                                                                         count(distinct(sent_message_id)) emails_sent
FROM
(SELECT campaign_id,
    sent_message_id,
    min(open_time_usec) AS open_time_usec
FROM gamma_reporting.Opens.all
WHERE cast(_partition_date AS int64) >= 20161111
AND campaign_id IN ($campaign_ids)
GROUP BY 1,
     2)
GROUP BY 1,
   2 )
GROUP BY 1 ) email_open ON cal.cal_date = email_open.email_open_date
LEFT JOIN
( SELECT email_send_date AS email_click_date,
                    max(CASE WHEN TYPE = 'welcome' THEN emails_sent END) AS welcome_click,
                                                                    max(CASE WHEN TYPE = 'onboarding' THEN emails_sent END) AS onboarding_click,
                                                                                                                       max(CASE WHEN TYPE = 'newsletter' THEN emails_sent END) AS newsletter_click
FROM
(SELECT date(timestamp_trunc(timestamp_micros(click_time_usec),DAY,"America/Los_Angeles")) email_send_date,
  CASE WHEN campaign_id IN ($onboarding_end) THEN 'onboarding' WHEN campaign_id IN ($welcome_id) THEN 'welcome' WHEN campaign_id IN ($newsletter_ids) THEN 'newsletter' END AS TYPE ,
                                                                                                                                                                         count(distinct(sent_message_id)) emails_sent
FROM
(SELECT campaign_id,
    sent_message_id,
    min(click_time_usec) AS click_time_usec
FROM gamma_reporting.Clicks.all
WHERE cast(_partition_date AS int64) >= 20161111
AND campaign_id IN ($campaign_ids)
GROUP BY 1,
     2)
GROUP BY 1,
   2 )
GROUP BY 1 ) email_click ON cal.cal_date = email_click.email_click_date
LEFT JOIN
( SELECT email_send_date AS opt_out_date,
                    max(CASE WHEN TYPE = 'welcome' THEN emails_sent END) AS welcome_optout,
                                                                    max(CASE WHEN TYPE = 'onboarding' THEN emails_sent END) AS onboarding_optout,
                                                                                                                       max(CASE WHEN TYPE = 'newsletter' THEN emails_sent END) AS newsletter_optout
FROM
(SELECT date(timestamp_trunc(timestamp_micros(opt_out_time_usec),DAY,"America/Los_Angeles")) email_send_date,
  CASE WHEN campaign_id IN ($onboarding_end) THEN 'onboarding' WHEN campaign_id IN ($welcome_id) THEN 'welcome' WHEN campaign_id IN ($newsletter_ids) THEN 'newsletter' END AS TYPE ,
                                                                                                                                                                         count(distinct(sent_message_id)) emails_sent
FROM gamma_reporting.OptOuts.all
WHERE cast(_partition_date AS int64) >= 20161111
AND campaign_id IN ($campaign_ids)
GROUP BY 1,
   2 )
GROUP BY 1 ) optout_email ON cal.cal_date = optout_email.opt_out_date ##################BEGIN ONBOARDING BREAKDOWN#########################################################################################################################################3
#########################################################

LEFT JOIN
( SELECT email_send_date AS email_newsletter_send,
                    max(CASE WHEN TYPE = 'music' THEN emails_sent END) AS music_send,
                                                                  max(CASE WHEN TYPE = 'tasks' THEN emails_sent END) AS tasks_send,
                                                                                                                max(CASE WHEN TYPE = 'answers' THEN emails_sent END) AS answers_send,
                                                                                                                                                                max(CASE WHEN TYPE = 'devices' THEN emails_sent END) AS devices_send,
max(CASE WHEN TYPE = 'personality' THEN emails_sent END) AS personality_send
FROM
(SELECT date(timestamp_trunc(timestamp_micros(sent_time_usec),DAY,"America/Los_Angeles")) email_send_date,
        CASE WHEN campaign_id = $music THEN 'music' WHEN campaign_id = $tasks THEN 'tasks' WHEN campaign_id = $answers THEN 'answers' WHEN campaign_id = $devices THEN 'devices' WHEN campaign_id = $personality THEN 'personality' END AS TYPE ,
count(distinct(sent_message_id)) emails_sent
 FROM lrs.healthcheck_SentMessages
WHERE campaign_id IN ($onboarding_ids)
AND date(timestamp_trunc(timestamp_micros(sent_time_usec),DAY,"America/Los_Angeles")) >= '2016-11-11'
 GROUP BY 1,
          2)
GROUP BY 1) onboarding_sends ON cal.cal_date = onboarding_sends.email_newsletter_send
LEFT JOIN
( SELECT email_send_date AS email_newsletter_click,
                          max(CASE WHEN TYPE = 'music' THEN emails_sent END) AS music_click,
                                                                              max(CASE WHEN TYPE = 'tasks' THEN emails_sent END) AS tasks_click,
                                                                                                                                  max(CASE WHEN TYPE = 'answers' THEN emails_sent END) AS answers_click,
                                                                                                                                                                                        max(CASE WHEN TYPE = 'devices' THEN emails_sent END) AS devices_click,
max(CASE WHEN TYPE = 'personality' THEN emails_sent END) AS personality_click
FROM
(SELECT date(timestamp_trunc(timestamp_micros(click_time_usec),DAY,"America/Los_Angeles")) email_send_date,
        CASE WHEN campaign_id = $music THEN 'music' WHEN campaign_id = $tasks THEN 'tasks' WHEN campaign_id = $answers THEN 'answers' WHEN campaign_id = $devices THEN 'devices' WHEN campaign_id = $personality THEN 'personality' END AS TYPE ,
count(distinct(sent_message_id)) emails_sent
 FROM lrs.healthcheck_ClickMessages
WHERE campaign_id IN ($onboarding_ids)
AND date(timestamp_trunc(timestamp_micros(click_time_usec),DAY,"America/Los_Angeles")) >= '2016-11-11'
 GROUP BY 1,
          2)
GROUP BY 1) onboarding_click ON cal.cal_date = onboarding_click.email_newsletter_click
LEFT JOIN
( SELECT email_send_date AS email_newsletter_open,
                          max(CASE WHEN TYPE = 'music' THEN emails_sent END) AS music_open,
                                                                              max(CASE WHEN TYPE = 'tasks' THEN emails_sent END) AS tasks_open,
                                                                                                                                  max(CASE WHEN TYPE = 'answers' THEN emails_sent END) AS answers_open,
                                                                                                                                                                                        max(CASE WHEN TYPE = 'devices' THEN emails_sent END) AS devices_open,
max(CASE WHEN TYPE = 'personality' THEN emails_sent END) AS personality_open
FROM
(SELECT date(timestamp_trunc(timestamp_micros(open_time_usec),DAY,"America/Los_Angeles")) email_send_date,
        CASE WHEN campaign_id = $music THEN 'music' WHEN campaign_id = $tasks THEN 'tasks' WHEN campaign_id = $answers THEN 'answers' WHEN campaign_id = $devices THEN 'devices' WHEN campaign_id = $personality THEN 'personality' END AS TYPE ,
count(distinct(sent_message_id)) emails_sent
 FROM lrs.healthcheck_OpenMessages
WHERE campaign_id IN ($onboarding_ids)
AND date(timestamp_trunc(timestamp_micros(open_time_usec),DAY,"America/Los_Angeles")) >= '2016-11-11'
 GROUP BY 1,
          2)
GROUP BY 1) onboarding_opens ON onboarding_opens.email_newsletter_open = cal.cal_date
LEFT JOIN
( SELECT email_send_date AS email_newsletter_opt_out,
                          max(CASE WHEN TYPE = 'music' THEN emails_sent END) AS music_opt_out,
                                                                              max(CASE WHEN TYPE = 'tasks' THEN emails_sent END) AS tasks_opt_out,
                                                                                                                                  max(CASE WHEN TYPE = 'answers' THEN emails_sent END) AS answers_opt_out,
                                                                                                                                                                                        max(CASE WHEN TYPE = 'devices' THEN emails_sent END) AS devices_opt_out,
max(CASE WHEN TYPE = 'personality' THEN emails_sent END) AS personality_opt_out
FROM
(SELECT date(timestamp_trunc(timestamp_micros(opt_out_time_usec),DAY,"America/Los_Angeles")) email_send_date,
        CASE WHEN campaign_id = $music THEN 'music' WHEN campaign_id = $tasks THEN 'tasks' WHEN campaign_id = $answers THEN 'answers' WHEN campaign_id = $devices THEN 'devices' WHEN campaign_id = $personality THEN 'personality' END AS TYPE ,
count(distinct(sent_message_id)) emails_sent
 FROM lrs.healthcheck_OptOutMessages
WHERE campaign_id IN ($onboarding_ids)
AND date(timestamp_trunc(timestamp_micros(opt_out_time_usec),DAY,"America/Los_Angeles")) >= '2016-11-11'
 GROUP BY 1,
          2)
GROUP BY 1) onboarding_opt_out ON cal.cal_date = onboarding_opt_out.email_newsletter_opt_out ### Partition dates on optout, optin, oobe_optin, optout_during_oobe, oobe_linked_account
LEFT JOIN
(SELECT partition_date AS preference_change_date ,
                        sum(optout) AS optouts ,
                                     sum(optin) AS optins ,
                                                 sum(oobe_optin) AS oobe_optin ,
                                                                  sum(optout_during_oobe) AS optout_during_oobe ,
                                                                                           sum(oobe_optin) + sum(optout_during_oobe) AS oobe_linked_account
FROM
 ( SELECT *
  FROM
    (SELECT base.opt_out_id ,
            chirp_email_prefs ###for each day
 ,
            CASE WHEN link_date BETWEEN '2016-11-04' AND '2016-11-09' THEN link_date WHEN link_date BETWEEN '2016-11-12' AND '2016-11-13' THEN link_date ELSE date_add(partition_date, interval -1 DAY) END AS partition_date ,
previous_email_prefs ####if ((day_before_email==null and today_email==true) or (day_before_email ==false and today_email ==true) then optin = true
 ,
CASE WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = TRUE THEN 1 WHEN previous_email_prefs = FALSE
     AND chirp_email_prefs = TRUE THEN 1 ELSE 0 END AS optin #ask about this line
 ####if ((day_before_email==null and today_email==false) or (day_before_email ==false and today_email ==true) then optout = true
 ,
                                                       CASE WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = FALSE THEN 1 WHEN previous_email_prefs IS TRUE
     AND chirp_email_prefs = FALSE THEN 1 ELSE 0 END AS optout ##ask about starting partition date
 #### if ((today_email == true) and (day_before_email == null) and (link_date == yesterday or link_date == starting_partition_date) then oobe_optin = true
 ,
                                                        CASE WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = TRUE
     AND link_date = date_add(partition_date, interval -1 DAY) THEN 1 WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = TRUE
     AND link_date BETWEEN '2016-11-04' AND '2016-11-10'
     AND partition_date = '2016-11-10' THEN 1 WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = TRUE
     AND link_date BETWEEN '2016-11-11' AND '2016-11-12'
     AND partition_date = '2016-11-14' THEN 1 ELSE 0 END AS oobe_optin #### if ((today_email == false) and (day_before_email == null) and (link_date == yesterday or link_date == starting_partition_date) then oobe_optin = false
 ,
                                                            CASE WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = FALSE
     AND link_date = date_add(partition_date, interval -1 DAY) THEN 1 WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = FALSE
     AND link_date BETWEEN '2016-11-04' AND '2016-11-10'
     AND partition_date = '2016-11-10' THEN 1 WHEN previous_email_prefs IS NULL
     AND chirp_email_prefs = FALSE
     AND link_date BETWEEN '2016-11-11' AND '2016-11-12'
     AND partition_date = '2016-11-14' THEN 1 ELSE 0 END AS optout_during_oobe
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
             4)) base) )
GROUP BY 1) optout ON cal.cal_date = optout.preference_change_date
LEFT JOIN #### Begin analysis of delink date
 #### ask about this

(SELECT cal_date delink_date ,
sum(delinked) delinks
FROM
( SELECT opt_out_id ,
   link_status ,
   cal_date ,
   CASE WHEN cal_date > prev_link_date
AND delink_partition_date IS NULL THEN 1 END AS delinked
FROM
( SELECT base.opt_out_id ,
     base.cal_date ,
     delink.link_date ,
     delink.chirp_email_prefs ,
     delink.partition_date AS delink_partition_date ,
                          CASE WHEN cal_date = delink.partition_date THEN 'active' ELSE 'unlinked' END AS link_status ,
                                                                                                      lag(delink.partition_date) over (partition BY base.opt_out_id
                                                                                                                                   ORDER BY cal_date) AS prev_link_date
FROM
( SELECT ids.opt_out_id ,
       cal_date ,
       end_date
FROM ( ####base table

    SELECT date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) cal_date
    FROM lrs.email_preferences a,
       a.device_data b
    JOIN lrs.devices v ON b.device_id = v.device_id
    WHERE $base_filter
    GROUP BY 1) cal,

 (SELECT a.opt_out_id,
         max(date(cast(substr(cast(a._partition_date AS string),0,4) AS int64), cast(substr(cast(a._partition_date AS string),5,2) AS int64) , cast(substr(cast(a._partition_date AS string),7,2) AS int64))) AS end_date
  FROM lrs.history.email_preferences.all a,
       unnest(a.device_data) b
  JOIN lrs.devices v ON b.device_id = v.device_id
  WHERE $base_filter
  GROUP BY 1) ids
GROUP BY 1,
       2,
       3) base
LEFT JOIN ####delink table

(SELECT a.opt_out_id ,
      chirp_email_prefs #### casting date into datetype
 ,
      date(cast(substr(cast(a._partition_date AS string),0,4) AS int64), cast(substr(cast(a._partition_date AS string),5,2) AS int64), cast(substr(cast(a._partition_date AS string),7,2) AS int64)) partition_date,
      ifnull(min(date(TIMESTAMP_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles"))), $null_date) AS link_date
FROM lrs.history.email_preferences.all a,
   unnest(a.device_data) b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1,
       2,
       3) delink ON base.cal_date = delink.partition_date
AND base.opt_out_id = delink.opt_out_id
WHERE cal_date NOT BETWEEN '2016-11-04' AND '2016-11-09'
AND cal_date NOT BETWEEN '2016-11-12' AND '2016-11-13') )
GROUP BY 1) delink ON cal.cal_date = delink.delink_date ##### OPT OUT ANALYSIS

LEFT JOIN
( SELECT *
FROM
(SELECT partition_date2,
  counterino AS total_email,
          c_opt_outs AS total_opt_out
FROM
( SELECT *
FROM(
   (SELECT cast(_partition_date AS int64) AS cal_date,
                                           $cast_date
  FROM lrs.onboarding_email_first.all
  GROUP BY 1,
         2) a
 LEFT JOIN
   (SELECT distinct(first_date) AS opt_date,
                                 count(distinct(optoutid)) AS counterino
  FROM
    (SELECT optoutid,
            min(cast(_partition_date AS int64)) AS first_date
     FROM lrs.onboarding_email_first.all
     GROUP BY 1)
  GROUP BY 1) emailed_users ON emailed_users.opt_date = a.cal_date)
LEFT JOIN
(SELECT partition_date,
      count(distinct(opt_out_id)) oobe_optout
FROM
 (SELECT cast(_partition_date AS int64) AS partition_date,
                                           opt_out_id
  FROM lrs.history.email_preferences.all
  WHERE chirp_email_prefs = FALSE
    AND opt_out_id IN
      ( SELECT optoutid
       FROM lrs.onboarding_email_first.all)
  GROUP BY 1,
           2)
GROUP BY 1) optout_after_oobe ON optout_after_oobe.partition_date = a.cal_date) sent_emails
LEFT JOIN
( SELECT partition_date,
     count(distinct(opt_out_id)) AS c_opt_outs
FROM
(SELECT base.opt_out_id ,
      chirp_email_prefs ###for each day
 ,
      CASE WHEN link_date BETWEEN '2016-11-04' AND '2016-11-09' THEN link_date WHEN link_date BETWEEN '2016-11-12' AND '2016-11-13' THEN link_date ELSE date_add(partition_date, interval -1 DAY) END AS partition_date ,
                                                                                                                                                                                                       previous_email_prefs ####if ((day_before_email==null and today_email==true) or (day_before_email ==false and today_email ==true) then optin = true
 ,
                                                                                                                                                                                                       CASE WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = TRUE THEN 1 WHEN previous_email_prefs = FALSE
AND chirp_email_prefs = TRUE THEN 1 ELSE 0 END AS optin #ask about this line
 ####if ((day_before_email==null and today_email==false) or (day_before_email ==false and today_email ==true) then optout = true
 ,
                                                CASE WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = FALSE THEN 1 WHEN previous_email_prefs IS TRUE
AND chirp_email_prefs = FALSE THEN 1 ELSE 0 END AS optout ##ask about starting partition date
 #### if ((today_email == true) and (day_before_email == null) and (link_date == yesterday or link_date == starting_partition_date) then oobe_optin = true
 ,
                                                 CASE WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = TRUE
AND link_date = date_add(partition_date, interval -1 DAY) THEN 1 WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = TRUE
AND link_date BETWEEN '2016-11-04' AND '2016-11-10'
AND partition_date = '2016-11-10' THEN 1 WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = TRUE
AND link_date BETWEEN '2016-11-11' AND '2016-11-12'
AND partition_date = '2016-11-14' THEN 1 ELSE 0 END AS oobe_optin #### if ((today_email == false) and (day_before_email == null) and (link_date == yesterday or link_date == starting_partition_date) then oobe_optin = false
 ,
                                                     CASE WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = FALSE
AND link_date = date_add(partition_date, interval -1 DAY) THEN 1 WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = FALSE
AND link_date BETWEEN '2016-11-04' AND '2016-11-10'
AND partition_date = '2016-11-10' THEN 1 WHEN previous_email_prefs IS NULL
AND chirp_email_prefs = FALSE
AND link_date BETWEEN '2016-11-11' AND '2016-11-12'
AND partition_date = '2016-11-14' THEN 1 ELSE 0 END AS optout_during_oobe
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
     JOIN
(SELECT OptOutId,
        _partition_date AS partition_date2
 FROM lrs.onboarding_email_first.all) g ON a.opt_out_id = g.OptOutId
     AND g.partition_date2 <= a._partition_date
     JOIN
(SELECT opt_out_id AS ooi
 FROM lrs.history.email_preferences.yesterday
 WHERE chirp_email_prefs = FALSE
 GROUP BY 1) j ON a.opt_out_id = j.ooi
WHERE $base_filter
     GROUP BY 1,
              2,
              3,
              4)) base)
WHERE optout = 1
GROUP BY 1) opt_outs ON opt_outs.partition_date = sent_emails.partition_date2 )) after_email_optout ON cal.cal_date = after_email_optout.partition_date2
WHERE cal.cal_date < current_date() ;


SET sql_dialect dremelsql;

 define permanent TABLE lrs.healthcheck_dashboard_v2 '/cns/iq-d/home/lrs/warehouse/marketing/dashboard/health_check2/main/data*';


SET sql_dialect dremelsql;

 GRANT OWNER ON TABLE lrs.healthcheck_dashboard_v2 TO 'mdb/lrs';

 GRANT reader ON TABLE lrs.healthcheck_dashboard_v2 TO 'mdb/lrs-viewers',
                                           'mdb/lrs-viewers-pii',
                                           'mdb/home-crm-dashboard',
                                           'home-crm-dashboard@google.com',
                                           'mdb/epsilon_analyst_group' ; ########END TOP METRICS#####################################################################################################################################################################
########BEGIN RATES ANALYSIS################################################################################################################################################################


SET sql_dialect googlesql;

 EXPORT DATA OPTIONS (path='/cns/iq-d/home/lrs/warehouse/marketing/dashboard/health_check2/rates/data' ,
          OWNER ='lrs' ,
     overwrite = TRUE) AS
SELECT *
FROM (
(SELECT cal_date,
#UNIX_MILLIS(timestamp_add(timestamp(cal_date), interval 8 hour)) as cal_date_ux,
'Linked Users' AS rate_segment, number
FROM ### Get range of cal dates

(SELECT date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) cal_date
FROM lrs.email_preferences a,
a.device_data b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1) cal
LEFT JOIN
( SELECT count(distinct(opt_out_id)) number , link_date
FROM
( SELECT a.opt_out_id ,
   chirp_email_prefs ,
   min(date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles"))) link_date
FROM lrs.email_preferences a,
a.device_data b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1,
   2)
GROUP BY 2) current_linked ON cal.cal_date = current_linked.link_date)
UNION ALL
( SELECT cal_date,
#UNIX_MILLIS(timestamp_add(timestamp(cal_date), interval 8 hour)) as cal_date_ux,
'Subscribers' AS rate_segment, number
FROM
(SELECT date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) cal_date
FROM lrs.email_preferences a,
a.device_data b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1) cal
LEFT JOIN
( SELECT current_sub_date,
 CASE WHEN current_sub_date ='2016-11-10' THEN subscribers ELSE subscribers - lag(subscribers) over (
                                                                                             ORDER BY current_sub_date) END number
FROM
( SELECT partition_date AS current_sub_date ,
                     count(opt_out_id) subscribers
FROM
( SELECT opt_out_id ,
     chirp_email_prefs ,
     partition_date ,
     previous_email_prefs ####ask why this is here
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
GROUP BY 1 )) subs ON subs.current_sub_date = cal.cal_date)
UNION ALL
( SELECT cal_date,
#UNIX_MILLIS(timestamp_add(timestamp(cal_date), interval 8 hour)) as cal_date_ux,
'Non-Subscribers' AS rate_segment,
number2-CASE WHEN number IS NULL THEN 0 ELSE number END AS number
FROM
(SELECT date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles")) cal_date
FROM lrs.email_preferences a,
a.device_data b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1) cal
LEFT JOIN
( SELECT current_sub_date,
 CASE WHEN current_sub_date ='2016-11-10' THEN subscribers ELSE subscribers - lag(subscribers) over (
                                                                                             ORDER BY current_sub_date) END number
FROM
( SELECT partition_date AS current_sub_date ,
                     count(opt_out_id) subscribers
FROM
( SELECT opt_out_id ,
     chirp_email_prefs ,
     partition_date ,
     previous_email_prefs ####ask why this is here
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
GROUP BY 1 )) subs ON subs.current_sub_date = cal.cal_date
LEFT JOIN
( SELECT count(distinct(opt_out_id)) number2 ,
 link_date
FROM
( SELECT a.opt_out_id ,
   chirp_email_prefs ,
   min(date(timestamp_trunc(timestamp_millis(b.time_linked_with_user_ms),DAY,"America/Los_Angeles"))) link_date
FROM lrs.email_preferences a,
a.device_data b
JOIN lrs.devices v ON b.device_id = v.device_id
WHERE $base_filter
GROUP BY 1,
   2)
GROUP BY 2) current_linked ON cal.cal_date = current_linked.link_date))
WHERE cal_date < current_date() ;


SET sql_dialect dremelsql;

 define permanent TABLE lrs.healthcheck_dashboard_v2_rates '/cns/iq-d/home/lrs/warehouse/marketing/dashboard/health_check2/rates/data*';


SET sql_dialect dremelsql;

 GRANT OWNER ON TABLE lrs.healthcheck_dashboard_v2_rates TO 'mdb/lrs';

 GRANT reader ON TABLE lrs.healthcheck_dashboard_v2_rates TO 'mdb/lrs-viewers',
                                                 'mdb/lrs-viewers-pii',
                                                 'mdb/home-crm-dashboard',
                                                 'home-crm-dashboard@google.com',
                                                 'mdb/epsilon_analyst_group' ;


SET sql_dialect googlesql;


SELECT *
FROM lrs.healthcheck_dashboard_v2 ;
