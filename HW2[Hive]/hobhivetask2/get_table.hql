ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE podkidysheval; --hob2022008 --podkidysheval

SELECT
       user,
       SUM(userSum) AS finalSum
FROM kkt_text
GROUP BY user
SORT BY finalSum DESC
LIMIT 1;

-- SELECT
--        user,
--        SUM(userSum) AS finalSum
-- FROM kkt_orc
-- GROUP BY user
-- SORT BY finalSum DESC
-- LIMIT 1;

-- SELECT
--        user,
--        SUM(userSum) AS finalSum
-- FROM kkt_parquet
-- GROUP BY user
-- SORT BY finalSum DESC
-- LIMIT 1;