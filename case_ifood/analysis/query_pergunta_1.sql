WITH faturamento_mensal AS (
  SELECT 
    pickup_year_month,
    SUM(total_amount) as faturamento_total
  FROM workspace.default.nyc_taxi_silver
  GROUP BY pickup_year_month
)

SELECT 
  AVG(faturamento_total) as media_faturamento_mensal_frota
FROM faturamento_mensal;