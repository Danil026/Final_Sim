Дашборд
Общий (DML)
Актуальность данных
Select max(purchase_datetime) from sale_market
Сумма продаж
Select sum(total_price) from sale_market
WHERE
  {{date_time}} [[AND {{product_id}}]]
  
Средний чек
WITH
  t1 AS (
    SELECT
      client_id,
      purchase_datetime::date AS purchase_date,
      SUM(total_price) AS order_sum
    FROM
      sale_market
    WHERE
      {{date_time}} [[AND {{product_id}}]]
    GROUP BY
      client_id,
      purchase_date
  )
SELECT
  AVG(order_sum) AS avg_check
FROM
  t1;
Выручка на клиента
SELECT
  SUM(total_price)::numeric / NULLIF(COUNT(DISTINCT client_id), 0)
FROM
  sale_market
WHERE
    {{date_time}} [[AND {{product_id}}]];
Ср. % скидки
SELECT
  SUM(discount_per_item * quantity) / NULLIF(SUM(price_per_item * quantity), 0)
FROM
  sale_market
WHERE
  {{date_time}} [[AND {{product_id}}]];
Количество клиентов
SELECT count (distinct  client_id) from sale_market
WHERE
  {{date_time}} [[AND {{product_id}}]]
Продано единиц товара
Select sum(quantity) from sale_market
WHERE
  {{date_time}} [[AND {{product_id}}]]
Распределение покупок по полу
Select gender, count(distinct client_id) from sale_market 
WHERE
{{date_time}} [[AND {{product_id}}]]
 GROUP by gender
Сумма продаж  VS пр.пер
Select purchase_datetime, sum(total_price) from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by purchase_datetime
Сумма продаж  VS пр.мес
Select DATE_TRUNC('month', purchase_datetime), sum(total_price) from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by DATE_TRUNC('month', purchase_datetime)
Количество покупателей VS предыдущий день
Select purchase_datetime, count (distinct client_id) from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by purchase_datetime
Кол-во покупат. VS пр. месяц
Select DATE_TRUNC('month', purchase_datetime), count (distinct client_id) from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by DATE_TRUNC('month', purchase_datetime)
Кол-во продаж VS пр.день
Select purchase_datetime, sum(quantity) from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by purchase_datetime
Кол-во продаж VS пр.мес
Select DATE_TRUNC('month', purchase_datetime), sum(quantity) from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by DATE_TRUNC('month', purchase_datetime)
График суммы продаж
WITH
  t1 AS (
    SELECT
      *, DATE_TRUNC('month', purchase_datetime) as dt,
      TO_CHAR(purchase_datetime, 'MM-YY') AS MY
    FROM
      sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  )
SELECT
  MY,
  sum(total_price) AS sum_purch
FROM
  t1
GROUP BY
  dt,MY order by dt desc
График количества покупателей
WITH
  t1 AS (
    SELECT
      *, DATE_TRUNC('month', purchase_datetime) as dt,
      TO_CHAR(purchase_datetime, 'MM-YY') AS MY
    FROM
      sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  )
SELECT
  MY,
  count(DISTINCT client_id) AS sum_purch
FROM
  t1
GROUP BY
  dt,MY order by dt desc
График количества проданных товаров
WITH
  t1 AS (
    SELECT
      *, DATE_TRUNC('month', purchase_datetime) as dt,
      TO_CHAR(purchase_datetime, 'MM-YY') AS MY
    FROM
      sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  )
SELECT
  MY,
  sum(quantity) AS purch_count
FROM
  t1
GROUP BY
  dt,MY order by dt desc
  Таблица кол-во покупателей, проданных товаров и суммы продаж
WITH
  t1 AS (
    SELECT
      *, DATE_TRUNC('month', purchase_datetime) as dt,
      TO_CHAR(purchase_datetime, 'MM-YYYY') AS MY
    FROM
      sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  )
SELECT
  MY,
  count(DISTINCT client_id) AS client_count,
  sum(quantity) AS purch_count,
  sum(total_price) as sum_purch
FROM
  t1
GROUP BY
  dt,MY order by dt desc

Retention
Rolling Retention,%
WITH 
  -- Шаг 1: находим первую покупку и считаем day_offset
  t1 AS (
    SELECT
      client_id,
      purchase_datetime,
      MIN(purchase_datetime) OVER (PARTITION BY client_id) AS first_purch,
      TO_CHAR(MIN(purchase_datetime) OVER (PARTITION BY client_id), 'YYYY-MM') AS cohort,
      (purchase_datetime - MIN(purchase_datetime) OVER (PARTITION BY client_id))::INT AS day_offset
    FROM sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  ),
  -- Шаг 2: фильтруем покупки после первой
  t2 AS (
    SELECT *
    FROM t1
    WHERE purchase_datetime >= first_purch
-- Итоговый расчёт 
-- Select * from t2
select
    cohort,
    round(count(distinct case when day_offset >= 0 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "0 (%)",
    round(count(distinct case when day_offset >= 1 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "1 (%)",
    round(count(distinct case when day_offset >= 3 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "3 (%)",
    round(count(distinct case when day_offset >= 7 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "7 (%)",
    round(count(distinct case when day_offset >= 14 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "14 (%)",
    round(count(distinct case when day_offset >= 30 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "30 (%)",
    round(count(distinct case when day_offset >= 60 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "60 (%)",
    round(count(distinct case when day_offset >= 90 then client_id end) * 100.0 / count(distinct case when day_offset >= 0 then client_id end), 2) as "90 (%)"
from t2
group by cohort

Rolling retantion (в числах)
WITH 
  -- Шаг 1: находим первую покупку и считаем day_offset
  t1 AS (
    SELECT
      client_id,
      purchase_datetime,
      MIN(purchase_datetime) OVER (PARTITION BY client_id) AS first_purch,
      TO_CHAR(MIN(purchase_datetime) OVER (PARTITION BY client_id), 'YYYY-MM') AS cohort,
      (purchase_datetime - MIN(purchase_datetime) OVER (PARTITION BY client_id))::INT AS day_offset
    FROM sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  ),
  -- Шаг 2: фильтруем покупки после первой
  t2 AS (
    SELECT *
    FROM t1
    WHERE purchase_datetime >= first_purch 
-- Итоговый расчёт 
-- Select * from t2
select
    cohort,
    count(distinct case when day_offset >= 0 then client_id end)  as "0 (%)",
    count(distinct case when day_offset >= 1 then client_id end) as "1 (%)",
    count(distinct case when day_offset >= 3 then client_id end)  as "3 (%)",
    count(distinct case when day_offset >= 7 then client_id end)  as "7 (%)",
   	count(distinct case when day_offset >= 14 then client_id end)  as "14 (%)",
    count(distinct case when day_offset >= 30 then client_id end)  as "30 (%)",
    count(distinct case when day_offset >= 60 then client_id end)  as "60 (%)",
    count(distinct case when day_offset >= 90 then client_id end)  as "90 (%)"
from t2
group by cohort

N-month Retention
WITH 
  -- Шаг 1: находим первую покупку и считаем day_offset
  t1 AS (
    SELECT
      client_id,
      purchase_datetime,
      MIN(purchase_datetime) OVER (PARTITION BY client_id) AS first_purch,
      TO_CHAR(MIN(purchase_datetime) OVER (PARTITION BY client_id), 'YYYY-MM') AS cohort,
      (purchase_datetime - MIN(purchase_datetime) OVER (PARTITION BY client_id))::INT AS day_offset
    FROM sale_market
	WHERE
  {{date_time}} [[AND {{product_id}}]]
  ),
  -- Шаг 2: фильтруем покупки после первой
  t2 AS (
    SELECT 
    *,
    CASE 
        WHEN purchase_datetime = first_purch 
            THEN 0
        ELSE 
            extract(year FROM age(purchase_datetime, first_purch)) * 12 
            + extract(month FROM age(purchase_datetime, first_purch)) + 1
    END AS months_diff
FROM t1
    WHERE purchase_datetime >= first_purch 
  )
-- Итоговый расчёт 
SELECT
    cohort,
    COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END) AS "1 покупка(абс)",
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 1 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "1_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 2 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "2_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 3 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "3_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 4 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "4_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 5 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "5_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 6 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "6_месяце,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 7 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "7_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 8 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "8_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 9 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "9_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 10 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "10_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 11 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "11_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff = 12 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS "12_месяц,%",
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN months_diff BETWEEN 1 AND 12 THEN client_id END)::numeric / 
         NULLIF(COUNT(DISTINCT CASE WHEN months_diff = 0 THEN client_id END), 0)) * 100,
        2
    ) AS total_clients
FROM t2
GROUP BY cohort
ORDER BY cohort;

Детализация
Количество покупателей, товаров и суммы продаж  по дням
select purchase_datetime, count(distinct client_id) as Count_client ,sum(quantity) as count_goods, sum(total_price) as sum_purch from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by purchase_datetime order by purchase_datetime desc
Топ товаров по сумме выручки
select product_id,  sum(total_price) as sum_purch from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by product_id order by sum(total_price) desc
select product_id,  sum(quantity) as sum_goods from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
Топ товаров по количеству продаж
group by product_id order by sum(quantity) desc
Топ покупателей по сумме выручки
select client_id,  sum(total_price) as sum_purch from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by client_id order by sum(total_price) desc
Топ покупателей по сумме выручки(штук)
select client_id,  sum(total_price) as sum_purch,sum(quantity) as cnt from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by client_id order by sum(total_price) desc
Топ продуктов по сред. % скидки
Select product_id,avg(discount_per_item*100/price_per_item) as "avg%disc" from sale_market 
WHERE
  {{date_time}} [[AND {{product_id}}]]
group by product_id order by "avg%disc" desc