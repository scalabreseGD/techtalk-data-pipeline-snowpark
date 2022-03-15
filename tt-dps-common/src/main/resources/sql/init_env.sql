create or replace database demo;
USE demo;
create schema TT_DPS;

CREATE OR REPLACE STORAGE INTEGRATION ADLS_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'eccbc9a0-56ca-4467-bacd-c7b7d36ef395'
  STORAGE_ALLOWED_LOCATIONS = ('azure://gdea2ttdps0adls2.blob.core.windows.net/tt-dps/');

desc storage integration ADLS_INTEGRATION;

grant create stage on schema TT_DPS to role accountadmin;

grant usage on integration ADLS_INTEGRATION to role accountadmin;

CREATE STAGE IF NOT EXISTS DEMO.TT_DPS.DEMO_STAGE
    STORAGE_INTEGRATION = ADLS_INTEGRATION
    url='azure://gdea2ttdps0adls2.blob.core.windows.net/tt-dps/'
