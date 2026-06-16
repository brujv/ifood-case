WITH passageiros_por_dia_hora AS (
  SELECT 
    DATE(tpep_pickup_datetime) as dia,
    HOUR(tpep_pickup_datetime) as hora,
    SUM(passenger_count) as total_passageiros_na_hora
  FROM workspace.default.nyc_taxi_silver
  WHERE pickup_year_month = '2023-05'
  GROUP BY DATE(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)
)

SELECT 
  hora,
  AVG(total_passageiros_na_hora) as media_passageiros_por_hora
FROM passageiros_por_dia_hora
GROUP BY hora
ORDER BY hora;