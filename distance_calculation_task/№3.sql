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


SELECT P_lat, 
	   P_lon,
	   count(Store) as N_Stores
  INTO ##N_stor
    FROM ##dist_cross
    WHERE Distance_km < 1
    GROUP BY P_lat, P_lon 

SELECT * 
FROM [test].[dbo].[Population] f1
LEFT OUTER JOIN  ##N_stor f2 ON f1.lat = f2.P_lat and f1.lon = f2.P_lon
ORDER BY metric_population DESC
