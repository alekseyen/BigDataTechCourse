ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE podkidysheval; --podkidysheval --hob2022008
DROP TABLE IF EXISTS kkt;

CREATE external TABLE kkt (
    id STRUCT<
        oid: String
    >,
    fsId STRING,
    kktRegId String,
    subtype String,
    receiveDate STRUCT<
		ddate : BIGINT
	>,
    protocolVersion INT,
    ofdId STRING,
    protocolSubversion INT,
    content STRUCT <
        fiscalDriveNumber: Bigint,
        operator: STRING,
        userInn: String,
        totalSum: Bigint,
        fiscalDocumentNumber: INT,
        dateTime: STRUCT<
                  date: Timestamp
        >
    >,
    documentId INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true',
    'mapping.id' = '_id',
    'mapping.oid' = '$oid',
    'mapping.date' = '$date'
)
LOCATION '/data/hive/fns2';

SELECT * FROM kkt LIMIT 50;
