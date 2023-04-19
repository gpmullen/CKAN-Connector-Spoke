create or replace table resources (
owner_org string NOT NULL
,database_name string not null
,schema_name string not null
,table_name string NOT NULL
,package_id string not NULL
,resource_id string not null
,presigned_url string NULL
,date_updated timestamp default CURRENT_TIMESTAMP()
);

create or replace stream resource_stream on table resources;