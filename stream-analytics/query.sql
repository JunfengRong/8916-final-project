/*
Here are links to help you get started with Stream Analytics Query Language:
Common query patterns - https://go.microsoft.com/fwLink/?LinkID=619153
Query language - https://docs.microsoft.com/stream-analytics-query/query-language-elements-azure-stream-analytics*/
SELECT 
	IoTHub.ConnectionDeviceId AS ConnectionDeviceId,
	AVG(iceThickness) AS AvgIceThickness,
	MIN(iceThickness) AS MinIceThickness,
	MAX(iceThickness) AS MaxIceThickness,
	AVG(surfaceTemperature) AS AvgSurfaceTemperature,
	MIN(surfaceTemperature) AS MinSurfaceTemperature,
	MAX(surfaceTemperature) AS MaxSurfaceTemperature,
	MAX(snowAccumulation) AS MaxSnowAccumulation,
	AVG(externalTemperature) AS AvgExternalTemperature,
	COUNT(*) AS ReadingCount,
	System.Timestamp AS EventTime,
	CASE
        WHEN AVG(iceThickness) >= 30 AND AVG(surfaceTemperature) <= -2 THEN 'Safe'
        WHEN AVG(iceThickness) >= 25 AND AVG(surfaceTemperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS SafetyStatus
INTO
	[iotoutput]
FROM
	[input]
GROUP BY
	IoTHub.ConnectionDeviceId, TumblingWindow(second, 300);

SELECT 
	IoTHub.ConnectionDeviceId AS ConnectionDeviceId,
	AVG(iceThickness) AS AvgIceThickness,
	MIN(iceThickness) AS MinIceThickness,
	MAX(iceThickness) AS MaxIceThickness,
	AVG(surfaceTemperature) AS AvgSurfaceTemperature,
	MIN(surfaceTemperature) AS MinSurfaceTemperature,
	MAX(surfaceTemperature) AS MaxSurfaceTemperature,
	MAX(snowAccumulation) AS MaxSnowAccumulation,
	AVG(externalTemperature) AS AvgExternalTemperature,
	COUNT(*) AS ReadingCount,
	System.Timestamp AS EventTime,
	CASE
        WHEN AVG(iceThickness) >= 30 AND AVG(surfaceTemperature) <= -2 THEN 'Safe'
        WHEN AVG(iceThickness) >= 25 AND AVG(surfaceTemperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS SafetyStatus
INTO
	[cosmosdb]
FROM
	[input]
GROUP BY
	IoTHub.ConnectionDeviceId, TumblingWindow(second, 300);
