select * from combined_calls cc;
select distinct  "disconnectType"
from combined_calls;

--2). How many disconnect types are there?
SELECT COUNT(DISTINCT "disconnectType") AS disconnect_type_count
FROM combined_calls;

--3). How many unique users are there?
SELECT COUNT(DISTINCT "UserId") AS unique_users
FROM combined_calls;

--4). Top 10 users who took calls (and call counts)
SELECT "UserId", COUNT(*) AS calls_taken
FROM combined_calls
GROUP BY "UserId"
ORDER BY calls_taken DESC
LIMIT 10;

--5). Average total handle time of a call (seconds + HH:MM:SS)
SELECT
  AVG("Total_Handle_Time") AS avg_handle_time_seconds,
  to_char(to_timestamp(AVG("Total_Handle_Time"))::time, 'HH24:MI:SS') AS avg_handle_time_hms
FROM combined_calls
WHERE "Total_Handle_Time" IS NOT NULL;


--6). Top 10 users with disconnectType = 'peer'
SELECT "UserId", COUNT(*) AS peer_disconnects
FROM combined_calls
WHERE lower("disconnectType") = 'peer'
GROUP BY "UserId"
ORDER BY peer_disconnects DESC
LIMIT 10;

--7). Top 10 users with highest ratio of peer disconnects to total calls
WITH totals AS (
  SELECT "UserId", COUNT(*) AS total_calls
  FROM combined_calls
  GROUP BY "UserId"
),
peer_counts AS (
  SELECT "UserId", COUNT(*) AS peer_count
  FROM combined_calls
  WHERE lower("disconnectType") = 'peer'
  GROUP BY "UserId"
),
client_counts AS (
  SELECT "UserId", COUNT(*) AS client_count
  FROM combined_calls
  WHERE lower("disconnectType") = 'client'
  GROUP BY "UserId"
)
SELECT
  t."UserId",
  t.total_calls,
  COALESCE(p.peer_count,0) AS peer_count,
  ROUND(COALESCE(p.peer_count,0)::numeric / t.total_calls, 4) AS peer_ratio,
  COALESCE(c.client_count,0) AS client_count,
  ROUND(COALESCE(c.client_count,0)::numeric / t.total_calls, 4) AS client_ratio
FROM totals t
LEFT JOIN peer_counts p ON p."UserId" = t."UserId"
LEFT JOIN client_counts c ON c."UserId" = t."UserId"
WHERE t.total_calls >= 5
ORDER BY peer_ratio DESC
LIMIT 10;

--8). How many inbound queue calls were received per month?
SELECT DATE_TRUNC('month', CAST("StartDateTime" AS TIMESTAMP)) AS month,
       COUNT(*) AS inbound_calls
FROM combined_calls
WHERE "CallType" = 'Inbound_Queue'
GROUP BY month
ORDER BY month;



--9). Average number of calls taken during Q4 by month
WITH monthly_calls AS (
    SELECT DATE_TRUNC('month', CAST("StartDateTime" AS TIMESTAMP)) AS month,
           COUNT(*) AS total_calls
    FROM combined_calls
    GROUP BY month
)
SELECT month,
       AVG(total_calls) OVER () AS avg_calls_q4
FROM monthly_calls
WHERE EXTRACT(QUARTER FROM month) = 4
ORDER BY month;

--10). % difference of inbound calls in 2023 vs 2024
WITH yearly AS (
    SELECT EXTRACT(YEAR FROM CAST("StartDateTime" AS TIMESTAMP)) AS year,
           COUNT(*) AS inbound_calls
    FROM combined_calls
    WHERE "CallType" = 'Inbound_Queue'
    GROUP BY year
)
SELECT 
    (SELECT inbound_calls FROM yearly WHERE year = 2024) AS calls_2024,
    (SELECT inbound_calls FROM yearly WHERE year = 2023) AS calls_2023,
    ROUND(
        (
            (SELECT inbound_calls FROM yearly WHERE year = 2024) -
            (SELECT inbound_calls FROM yearly WHERE year = 2023)
        ) * 100.0 /
        (SELECT inbound_calls FROM yearly WHERE year = 2023),
    2) AS pct_difference;



--11). Month and year with the most inbound queue calls
SELECT DATE_TRUNC('month', CAST("StartDateTime" AS TIMESTAMP)) AS month,
       COUNT(*) AS inbound_calls
FROM combined_calls cc 
WHERE "CallType" = 'Inbound_Queue'
GROUP BY month
ORDER BY inbound_calls DESC
LIMIT 1;

--12). Calls taken per month during 2023 and 2024
SELECT DATE_TRUNC('month', CAST("StartDateTime" AS TIMESTAMP)) AS month,
       COUNT(*) AS calls_taken
FROM combined_calls
WHERE EXTRACT(YEAR FROM CAST("StartDateTime" AS TIMESTAMP)) IN (2023, 2024)
GROUP BY month
ORDER BY month;



























