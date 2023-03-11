create or replace table control_spoke (
notes string NOT NULL
,accesslevel string NOT NULL
,contact_name string NOT NULL
,contact_email string NOT NULL
,rights string NOT NULL
,accrualperiodicity string NOT NULL
,tag_string string NOT NULL
,owner_org string NOT NULL
,database_name string not null
,schema_name string not null
,table_name string NOT NULL
,presigned_url string NULL);

create or replace stream control_spoke_stream on table control_spoke;