WITH input_bucketized AS (
	SELECT *, cast(DATETIME / INTERVAL '30' MINUTE AS bigint) AS bk
	FROM CRIMES
),
ranges AS (
	SELECT R.bk as bk_s, M.bk as bk_e
	FROM input_bucketized AS R, input_bucketized AS M
	WHERE R.bk = M.bk
		AND R.PRIMARY_TYPE = 'ROBBERY'
		AND M.PRIMARY_TYPE = 'MOTOR VEHICLE THEFT'
		AND M.LON BETWEEN R.LON - 0.05 AND R.LON + 0.05
		AND M.LAT BETWEEN R.LAT - 0.02 AND R.LAT + 0.02
		AND M.DATETIME - R.DATETIME <= INTERVAL '30' MINUTE
	UNION
	SELECT R.bk as bk_s, M.bk as bk_e
	FROM input_bucketized AS R, input_bucketized AS M
	WHERE R.bk + 1 = M.bk
		AND R.PRIMARY_TYPE = 'ROBBERY'
		AND M.PRIMARY_TYPE = 'MOTOR VEHICLE THEFT'
		AND M.LON BETWEEN R.LON - 0.05 AND R.LON + 0.05
		AND M.LAT BETWEEN R.LAT - 0.02 AND R.LAT + 0.02
		AND M.DATETIME - R.DATETIME <= INTERVAL '30' MINUTE
),
buckets AS (
	SELECT DISTINCT bk FROM ranges
	CROSS JOIN UNNEST(sequence(ranges.bk_s, ranges.bk_e)) AS t(bk)
), 
prefilter AS (
	SELECT i.* FROM input_bucketized AS i, buckets AS b
	WHERE i.bk = b.bk
)
SELECT * FROM prefilter MATCH_RECOGNIZE (
	ORDER BY datetime
    MEASURES
        R.id AS RID,
        B.id AS BID,
        M.id AS MID,
        COUNT(Z.id) AS GAP
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (R Z B Z M)
    DEFINE
        R AS R.primary_type = 'ROBBERY',
        B AS B.primary_type = 'BATTERY'
             AND B.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
             AND B.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02,
        M AS M.primary_type = 'MOTOR VEHICLE THEFT'
             AND M.lon BETWEEN R.lon - 0.05 AND R.lon + 0.05
             AND M.lat BETWEEN R.lat - 0.02 AND R.lat + 0.02
             AND M.datetime - R.datetime <= INTERVAL '30' MINUTE
)