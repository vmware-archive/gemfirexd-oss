--This script is for creating batch job details table
DROP TABLE IF EXISTS RTRADM.BATCH_JOB_DETAIL;

CREATE TABLE RTRADM.BATCH_JOB_DETAIL
(
   JOB_INSTANCE_ID bigint,
   JOB_NAME varchar(50),
   LAST_JOB_START_TIME timestamp
);
