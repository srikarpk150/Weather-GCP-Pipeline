#Query to check the recently generated data in current_weather table 
SELECT name as CITY
FROM `fa24-i535-vpothapr-weatherpipe.weather_api_data.current_weather`
WHERE timestamp = (SELECT MAX(timestamp) FROM `fa24-i535-vpothapr-weatherpipe.weather_api_data.current_weather`);

#Average temperature By City

SELECT name, AVG(main_temp) AS avg_temp
FROM `fa24-i535-vpothapr-weatherpipe.weather_api_data.forecast_weather`
GROUP BY name
ORDER BY avg_temp DESC;

#City vs Daylight Hours
SELECT name AS city,(sys_sunset - sys_sunrise) / 3600 AS daylight_hours
FROM `fa24-i535-vpothapr-weatherpipe.weather_api_data.current_weather`
ORDER BY daylight_hours DESC;

#City vs Average Wind Speed
SELECT name AS city,AVG(wind_speed) AS avg_wind_speed
FROM  `fa24-i535-vpothapr-weatherpipe.weather_api_data.forecast_weather`
GROUP BY city
ORDER BY avg_wind_speed DESC;

