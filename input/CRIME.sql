SELECT *
FROM Crimes
MATCH_RECOGNIZE (
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