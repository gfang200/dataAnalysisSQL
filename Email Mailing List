SET materialize_owner_group lrs;
SET materialize_overwrite TRUE;
define macro modulo WHEN mod(fingerprint2011(cast(gaia_id AS string)),200) BETWEEN $1 AND $2 THEN $3; #segmentation based off Gaia_id.  May show bias to a group initially but will level off over time.
define macro languages en','en-us;
define macro countries us;
define macro email_launch_date 2016-11-04 ;
define macro campaign_id 10035542;

MATERIALIZE "csv:/encrypted/sid=70831:mkey=gamma.csv.uploader:mdb=bulk-email/namespace/bulk-email/gamma/csv/no_schedule/$campaign_id/$campaign_id-${YYYYMMDD}@1.csv header:true" AS #select reporting_signals.signal_1 from gamma_reporting.SentMessages.all where campaign_id = 10023933;

SELECT * from
  (SELECT a.OptOutId AS OptOutId , b.EmailAddress AS EmailAddress , a.LanguagePreference AS LanguagePreference , a.country AS country , a.OptOutId AS Reporting1 , Control
   FROM
     (SELECT opt_out_id AS OptOutId , device_data.device_id AS device_id, LanguagePreference AS LanguagePreference , country AS Country , CASE WHEN newsletter_segmentation IN ('campaign control', 'universal control') THEN TRUE ELSE FALSE END AS Control
      FROM flatten(lrs.email_preferences, device_data.device_id)
      WHERE datediff(time_usec_to_day(now()), time_usec_to_day(device_data.time_linked_with_user_ms * 1000)) > 21
        AND time_usec_to_day(device_data.time_linked_with_user_ms *1000) >= parse_time_usec('$email_launch_date') #Launch date of Goggle Home emails

        AND chirp_email_prefs = TRUE
        AND lower(languagepreference) IN ('$languages')
        AND lower(country) IN ('$countries')
        AND country_incorrect != TRUE ) a
   JOIN
     (SELECT opt_out_id AS optoutid , email AS EmailAddress, gaia_id AS gaia_id
      FROM lrs.email_preferences_pii
      WHERE email IS NOT NULL
      GROUP BY 1,2,3 ) b ON a.optoutid = b.optoutid
   JOIN
     (SELECT device_id , model_name
      FROM lrs.devices
      WHERE lower(model_name) = 'google home'
        AND lower(product_name) = 'pineapple'
      GROUP BY 1,2 ) c ON a.device_id = c.device_id
   GROUP BY 1,2,3,4,5,6) LIMIT 1
