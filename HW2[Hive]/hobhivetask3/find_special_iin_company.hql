ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE podkidysheval; --hob2022008 --podkidysheval

DROP TABLE IF EXISTS parsed_kkt;
CREATE TABLE parsed_kkt AS
SELECT
    content.userInn as iin,
    DAY(content.datetime.date) as day,
    COALESCE(content.totalSum, 0) as userSum
FROM kkt
WHERE content.userInn IS NOT NULL;

DROP VIEW IF EXISTS day_sums;
CREATE VIEW day_sums AS
SELECT
    iin,
    day,
    sum(userSum) as daySum,
    MAX(SUM(userSum)) OVER (PARTITION BY iin) as maxSum
FROM parsed_kkt
GROUP BY iin, day;

SELECT
    iin,
    day,
    daySum
FROM day_sums
WHERE maxSum == daySum and day is not NULL;
