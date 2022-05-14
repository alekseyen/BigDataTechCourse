ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE podkidysheval; --hob2022008 --podkidysheval

DROP TABLE IF EXISTS kkt_text;
CREATE TABLE kkt_text
STORED AS TEXTFILE
AS SELECT
        content.userInn as user,
        content.totalSum as userSum
   FROM kkt WHERE subtype = "receipt";


DROP TABLE IF EXISTS kkt_orc;
CREATE TABLE kkt_orc
STORED AS ORC
AS SELECT
        content.userInn as user,
        content.totalSum as userSum
   FROM kkt WHERE subtype = "receipt";

DROP TABLE IF EXISTS kkt_parquet;
CREATE TABLE kkt_parquet
STORED AS PARQUET
AS SELECT
        content.userInn as user,
        content.totalSum as userSum
   FROM kkt WHERE subtype = "receipt";

