ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE podkidysheval; --hob2022008 --podkidysheval

DROP VIEW IF EXISTS avg_first_half_day_profit;
CREATE VIEW avg_first_half_day_profit AS
SELECT
    content.userInn as iin,
    AVG(COALESCE(content.totalSum, 0)) as avgSum
FROM kkt
WHERE HOUR(content.datetime.date) < 13
GROUP BY content.userInn;

DROP VIEW IF EXISTS avg_second_half_day_profit;
CREATE VIEW avg_second_half_day_profit AS
SELECT
    content.userInn as iin,
    AVG(COALESCE(content.totalSum, 0)) as avgSum
FROM kkt
WHERE HOUR(content.datetime.date) >= 13
GROUP BY content.userInn;

SELECT
       avg_first_half_day_profit.iin,
       round(avg_first_half_day_profit.avgSum) as first_half_profit_avg,
       round(avg_second_half_day_profit.avgSum) as second_half_profit_avg
FROM avg_first_half_day_profit JOIN avg_second_half_day_profit
    ON avg_first_half_day_profit.iin = avg_second_half_day_profit.iin
WHERE avg_first_half_day_profit.avgSum > avg_second_half_day_profit.avgSum
ORDER BY first_half_profit_avg
LIMIT 50;
