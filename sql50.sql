
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';


create database temperature;

/*  
!!!Dates with higher temperatures compared to its previous dates!!!


Weather table
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| recordDate    | date    |
| temperature   | int     |
+---------------+---------+
id is the column with unique values for this table. This table contains information about the temperature on a certain day.

Example 1:
----------
Weather table:
+----+------------+-------------+
| id | recordDate | temperature |
+----+------------+-------------+
| 1  | 2015-01-01 | 10          |
| 2  | 2015-01-02 | 25          |
| 3  | 2015-01-03 | 20          |
| 4  | 2015-01-04 | 30          |
+----+------------+-------------+
*/


create table temperature.public.temperature (
id int,
recorddate date,
temperature int
);


INSERT INTO data.public.temperature (id, recordDate, temperature)
SELECT 
  SEQ4() as id,
  DATEADD(day, UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) as recordDate,
  UNIFORM(0, 100, RANDOM()) as temperature
FROM TABLE(GENERATOR(ROWCOUNT => 200));



/*
1. Generación de Filas
 - FROM TABLE(GENERATOR(ROWCOUNT => 200)): Esta es la "fábrica". Le dice a Snowflake que cree un espacio de trabajo temporal con 200 filas vacías para que las funciones del SELECT tengan donde trabajar.

2. Las Columnas (El Resultado)
----------+--------------+----------------------------------------
Columna	    Función	        Qué hace
----------+--------------+----------------------------------------
id	         SEQ4()	        Genera un número secuencial único de 4 bytes para cada fila (0, 1, 2, 3...). 
                            Es ideal para crear llaves primarias rápidas.
----------+--------------+----------------------------------------
recordDate   DATEADD(...)	Crea una fecha aleatoria. 
                            Toma la fecha actual (CURRENT_DATE()) y le suma un número de días al azar entre 0 y 365.
----------+--------------+----------------------------------------
temperature	 UNIFORM(...)	Genera un número entero aleatorio entre 0 y 100 para simular una lectura de temperatura.
----------+--------------+----------------------------------------

*/



/*
Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday). Return the result table in any order.
The result format is in the following example.

Output: 
+----+
| id |
+----+
| 2  |
| 4  |
+----+

*/





-- Use Lag() window function to get the previous day's temperature
WITH
temp1 as (
select 
*,
LAG(recordDate) OVER (order by recordDate) AS prev_recordDate,
LAG(temperature) OVER (order by recordDate) AS prev_temperature
from data.public.temperature

qualify 
temperature > prev_temperature AND
recordDate = prev_recordDate + INTERVAL '1 day'
-- filter at the beginning of the CTE

order by recorddate desc
)
SELECT id   ,recordDate, temperature, prev_recordDate, prev_temperature 
FROM temp1
--where recordDate = prev_recordDate + INTERVAL '1 day'





/*
Output: 
+----+
| id |
+----+
| 2  |
| 4  |
+----+

*/
