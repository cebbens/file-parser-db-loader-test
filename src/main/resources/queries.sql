SELECT ip, COUNT(ip) AS request_count FROM parser.log_entries WHERE date BETWEEN ? AND ? GROUP BY ip HAVING request_count > ? ORDER BY request_count;
-- Example
SELECT ip, COUNT(ip) AS request_count FROM parser.log_entries WHERE date BETWEEN '2017-01-01.13:00:00' AND '2017-01-01.14:00:00' GROUP BY ip HAVING request_count > 200 ORDER BY request_count;


SELECT date, request, status, user_agent FROM parser.log_entries WHERE ip = ? ORDER BY date;
-- Example
SELECT date, request, status, user_agent FROM parser.log_entries WHERE ip = '192.168.169.194' ORDER BY date;
