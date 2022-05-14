ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
ADD file ./filter_criminal.py;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE podkidysheval; --hob2022008 --podkidysheval

DROP VIEW IF EXISTS transactions;
CREATE VIEW transactions AS
SELECT
    content.userInn as iin,
    kktregid,
    subtype,
    UNIX_TIMESTAMP(content.datetime.date) as time
FROM kkt
WHERE content.userInn is not NULL
ORDER BY iin, kktregid, time, subtype;


SELECT
    TRANSFORM(iin, kktregid, subtype) USING './filter_criminal.py' AS (iin String)
FROM transactions
ORDER BY iin
LIMIT 50;
