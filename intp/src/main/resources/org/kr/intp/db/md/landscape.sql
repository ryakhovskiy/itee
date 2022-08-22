CREATE ROW TABLE $$SCHEMA$$.LANDSCAPE
(
       DEV_NAME VARCHAR(100) CS_STRING,
       DEV_INTP_HOST VARCHAR(50) CS_STRING,
       DEV_INTP_PORT INT CS_INT,
       QA_NAME VARCHAR(100) CS_STRING,
       QA_INTP_HOST VARCHAR(50) CS_STRING,
       QA_INTP_PORT INT CS_INT,
       PROD_NAME VARCHAR(100) CS_STRING,
       PROD_INTP_HOST VARCHAR(50) CS_STRING,
       PROD_INTP_PORT INT CS_INT
)