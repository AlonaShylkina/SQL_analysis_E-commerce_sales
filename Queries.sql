-- this query calculates the percentage of sessions with purchases for each country
SELECT 
  sp.country, 
  ROUND(COUNT(o.ga_session_id) / COUNT(sp.ga_session_id) * 100, 2) AS session_with_orders_percent, 
  COUNT(sp.ga_session_id) AS session_cnt
FROM `data-analytics-mate.DA.session_params` sp 
LEFT JOIN `data-analytics-mate.DA.order` o
ON sp.ga_session_id = o.ga_session_id
GROUP BY sp.country
ORDER BY session_cnt DESC;


-- this query finds the letter type (letter_type) with the highest open rate in the United States
SELECT 
  es.letter_type, 
  COUNT(DISTINCT es.id_message) AS email_sent_cnt, 
  COUNT(DISTINCT eo.id_message) AS email_open_cnt,
  COUNT(DISTINCT eo.id_message) / COUNT(DISTINCT es.id_message) AS open_rate
FROM `data-analytics-mate.DA.email_sent` es 
LEFT JOIN `data-analytics-mate.DA.email_open` eo
ON es.id_message = eo.id_message 
LEFT JOIN `data-analytics-mate.DA.account_session` acs
ON es.id_account = acs.account_id
LEFT JOIN `data-analytics-mate.DA.session_params` sp
ON acs.ga_session_id = sp.ga_session_id
WHERE sp.country = 'United States'
GROUP BY es.letter_type
ORDER BY open_rate DESC
LIMIT 1;


/* this query calculates the number of sold products and the total sales revenue in the "Beds" 
category for each country on the continent of Europe */
SELECT 
  sp.country, 
  SUM(p.price) AS revenue, 
  COUNT(p.item_id) AS count_of_orders
FROM `data-analytics-mate.DA.product` p JOIN `data-analytics-mate.DA.order` o
ON p.item_id = o.item_id
JOIN `data-analytics-mate.DA.session_params` sp
ON o.ga_session_id = sp.ga_session_id
WHERE sp.continent = 'Europe' AND p.category = 'Beds'
GROUP BY sp.country
ORDER BY count_of_orders DESC
LIMIT 1;


--this query determines which continent has the highest revenue from purchases made on mobile devices
SELECT 
  sp.continent, 
  SUM(p.price) AS revenue,
  ROUND(SUM(CASE WHEN sp.device = 'mobile' THEN p.price END) / SUM(p.price) * 100, 2) AS revenue_from_mobile_percent
FROM `data-analytics-mate.DA.product` p JOIN `data-analytics-mate.DA.order` o
ON p.item_id = o.item_id
JOIN `data-analytics-mate.DA.session_params` sp
ON o.ga_session_id = sp.ga_session_id
GROUP BY sp.continent
ORDER BY revenue DESC;


--this query calculates the percentage of cumulative revenue achieved from cumulative goals (predict) by day
SELECT 
  date,
  SUM(revenue) AS revenue,
  SUM(SUM(revenue)) OVER (ORDER BY date) AS acc_revenue,
  SUM(predict) AS predict,
  SUM(SUM(predict)) OVER (ORDER BY date) AS acc_predict,
  ROUND(SUM(SUM(revenue)) OVER (ORDER BY date) / SUM(SUM(predict)) OVER (ORDER BY date) * 100, 2) AS percent_of_completion   
FROM(
SELECT s.date, SUM(p.price) AS revenue, 0 AS predict
FROM `data-analytics-mate.DA.product` p
JOIN `data-analytics-mate.DA.order` o
ON p.item_id = o.item_id
JOIN `data-analytics-mate.DA.session` s
ON o.ga_session_id = s.ga_session_id
GROUP BY s.date
UNION ALL
SELECT date, 0 AS revenue, predict
FROM `data-analytics-mate.DA.revenue_predict`) AS info
GROUP BY date
ORDER BY date


--this query selects sessions where the language field contains English with specific qualifiers: us, gb, ca
SELECT 
  substr(language, -2) AS en_type, 
  COUNT(ga_session_id) AS session_cnt
FROM `data-analytics-mate.DA.session_params`
WHERE language LIKE 'en-%'
GROUP BY language
ORDER BY session_cnt DESC;


--this query determines the percentage of sessions where the language is not specified (the language field is empty)
SELECT 
  browser, 
  COUNT(ga_session_id) AS session_cnt,
  COUNT(CASE WHEN language IS NULL THEN ga_session_id END) AS session_cnt_with_empty_language,
  ROUND(COUNT(CASE WHEN language IS NULL THEN ga_session_id END) / COUNT(ga_session_id) * 100, 2) AS session_cnt_with_empty_language_percent
FROM `data-analytics-mate.DA.session_params`
GROUP BY browser;


--this query outputs the total revenue and advertising expenses by day in a single table
SELECT 
  s.date,
  'revenue' AS type, 
  SUM(p.price) AS value
FROM `DA.order` o JOIN `DA.session` s ON o.ga_session_id = s.ga_session_id
JOIN `DA.product` p ON o.item_id = p.item_id
GROUP BY s.date
UNION ALL
SELECT date, 'cost' AS type, cost
FROM `DA.paid_search_cost`
ORDER BY date, type DESC;


/* this query counts the number of events with the type user_engagement, but only for those sessions 
where there were more than 2 events in total within the session */
SELECT COUNT(ep.ga_session_id) AS session_cnt
FROM `data-analytics-mate.DA.event_params` ep
JOIN
(SELECT ga_session_id FROM `data-analytics-mate.DA.event_params`
GROUP BY ga_session_id
HAVING COUNT(event_name) > 2) AS sessions
ON ep.ga_session_id = sessions.ga_session_id
WHERE event_name = 'user_engagement';


/*this query helps to extract the last part of the size from the product description for those items where the size 
is specified in the format width x length cm or height x width x length */
SELECT
  short_description,
  CASE WHEN regexp_contains(short_description, r'\d+x\d+\s*cm') AND NOT regexp_contains(short_description, r'\d+x\d+x\d+\s*cm') 
  THEN regexp_extract(short_description, r'\d+x(\d+\s*cm)')
  ELSE NULL
  END AS size
FROM `data-analytics-mate.DA.product`;


--this query groups revenue and expenses by year and month
SELECT
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(MONTH FROM date) AS month,
  SUM(revenue) AS revenue,
  SUM(cost) AS cost
FROM(
  SELECT s.date, SUM(p.price) AS revenue, 0 AS cost
  FROM `DA.order` o JOIN `DA.session` s ON o.ga_session_id = s.ga_session_id
  JOIN `DA.product` p ON o.item_id = p.item_id
  GROUP BY s.date
  UNION ALL
  SELECT date, 0 AS revenue, cost
  FROM `DA.paid_search_cost`
  ORDER BY date
  )
GROUP BY year, month;


/* This query determines the percentage of emails out of the total number sent to each account within each month and 
identifies the date of the first and last email sent for each account in the month */
SELECT DISTINCT
  sent_month,
  id_account,
  sent_message / SUM(sent_message) OVER (PARTITION BY sent_month) * 100 AS sent_msg_percent_from_this_month,
  first_sent_date,
  last_sent_date
FROM (
SELECT
  DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
  es.id_account,
  COUNT(DISTINCT es.id_message) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS sent_message,
  MIN(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS first_sent_date,
  MAX(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS last_sent_date
FROM `data-analytics-mate.DA.email_sent` es
JOIN `data-analytics-mate.DA.account_session` ac
ON es.id_account = ac.account_id
JOIN `data-analytics-mate.DA.session` s
ON ac.ga_session_id = s.ga_session_id
) AS first_info
ORDER BY sent_month
