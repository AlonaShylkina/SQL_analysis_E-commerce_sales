/* This query collects data that will help analyze the dynamics of account creation, 
user activity based on emails (sending, opening, clicks), and evaluate behavior in 
categories such as sending intervals, account verification, and subscription status. 
The data will enable comparisons of activity across countries, identification of key markets, 
and segmentation of users based on various parameters. */

-- create an CTE with the combined data by accounts and emails
WITH main_metrics AS(
SELECT date,
      country,
      SUM(send_interval) AS send_interval,
      SUM(is_verified) AS is_verified,
      SUM(is_unsubscribed) AS is_unsubscribed,
      SUM(account_cnt) AS account_cnt,
      SUM(sent_msg) AS sent_msg,
      SUM(open_msg) AS open_msg,
      SUM(visit_msg) AS visit_msg,
FROM( 
-- the data by accounts   
SELECT s.date AS date,
      sp.country AS country,
      a.send_interval AS send_interval,
      a.is_verified AS is_verified,
      a.is_unsubscribed AS is_unsubscribed,
      COUNT(DISTINCT a.id) AS account_cnt,
      0 AS sent_msg,
      0 AS open_msg,
      0 AS visit_msg
FROM `DA.account` a
JOIN `DA.account_session` acs
ON a.id = acs.account_id
JOIN `DA.session` s
ON acs.ga_session_id = s.ga_session_id
JOIN `DA.session_params` sp
ON s.ga_session_id = sp.ga_session_id
GROUP BY s.date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
UNION ALL --full union of data by accounts and emails
--output the data by emails
SELECT
      DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
      sp.country AS country,
      0 AS send_interval,
      0 AS is_verified,
      0 AS is_unsubscribed,
      0 AS account_cnt,
      COUNT(DISTINCT es.id_message) AS sent_msg,
      COUNT(DISTINCT eo.id_message) AS open_msg,
      COUNT(DISTINCT ev.id_message) AS visit_msg
FROM `data-analytics-mate.DA.email_sent` es
LEFT JOIN `data-analytics-mate.DA.email_open` eo
ON es.id_message = eo.id_message
LEFT JOIN `data-analytics-mate.DA.email_visit` ev
ON es.id_message = ev.id_message
JOIN `data-analytics-mate.DA.account_session` acs
ON es.id_account = acs.account_id
JOIN `data-analytics-mate.DA.session` s
ON acs.ga_session_id = s.ga_session_id
JOIN `DA.session_params` sp
ON s.ga_session_id = sp.ga_session_id
GROUP BY date, sp.country) as metrics
GROUP BY date, country),

-- extract the calculated data into a separate CTE
ranks_by_country AS(
SELECT country,
      SUM(account_cnt) AS total_country_account_cnt,
      SUM(sent_msg) AS total_country_sent_cnt,
      DENSE_RANK() OVER (ORDER BY SUM(account_cnt) DESC) AS rank_total_country_account_cnt,
      DENSE_RANK() OVER (ORDER BY SUM(sent_msg) DESC) AS rank_total_country_sent_cnt
FROM main_metrics
GROUP BY country)

-- collect all the necessary data into the final table
SELECT
 m.date,
 m.country,
 m.send_interval,
 m.is_verified,
 m.is_unsubscribed,
 m.account_cnt,
 m.sent_msg,
 m.open_msg,
 m.visit_msg,
 r.total_country_account_cnt,
 r.total_country_sent_cnt,
 r.rank_total_country_account_cnt,
 r.rank_total_country_sent_cnt
FROM main_metrics m
JOIN ranks_by_country r
ON m.country = r.country
WHERE r.rank_total_country_account_cnt <= 10 OR r.rank_total_country_sent_cnt <= 10;
