SELECT Store, 
       f1.lat as P_lat, 
	   f2.lat as S_lat, 
	   f1.lon as P_lon, 
	   f2.long as S_lon, 
	   metric_population, 
	   Metric_Store, 
       (6371 * ACOS(SIN(RADIANS(f1.lat)) * SIN(RADIANS(f2.lat)) + COS(RADIANS(f1.lat)) * COS(RADIANS(f2.lat)) * COS(RADIANS(f1.lon - f2.long)))) as Distance_km
  INTO ##dist_cross
    FROM test.dbo.Population f1
    CROSS JOIN test.dbo.Stores f2 

SELECT f1.Store, 
       P_lat, 
	   P_lon, 
	   f1.Distance_km
  INTO #pop_best 
    FROM ##dist_cross f1
    INNER JOIN (SELECT Store, 
	                   min(Distance_km) as Distance_km 
			      FROM #dist_cross GROUP BY Store) as f2 on f1.Store = f2.Store and f1.Distance_km = f2.Distance_km

SELECT f1.Store, 
       f2.lat as S_lat, 
	   f2.long as S_lon, 
	   f1.P_lat, 
	   f1.P_lon, 
	   f2.Metric_Store, 
	   f3.metric_population,
       (f2.Metric_Store - AVG(f2.Metric_Store) OVER ()) * (f3.metric_population - AVG(f3.metric_population) OVER ()) as num,
       POWER(f2.Metric_Store - AVG(f2.Metric_Store) OVER (), 2)  den_x, 
       POWER(f3.metric_population - AVG(f3.metric_population) OVER (), 2) den_y, 
       Distance_km
  INTO ##t1
    FROM #pop_best f1
      LEFT OUTER JOIN test.dbo.Stores f2 on f1.Store=f2.Store
      LEFT OUTER JOIN test.dbo.Population f3 on f1.P_lat = f3.lat and f1.P_lon=f3.lon

SELECT 'r' as Coef, 
       ROUND(sum(num)/ sqrt(sum(den_x)*sum(den_y)), 3) as Value
  INTO ##koef_P
    FROM ##t1



