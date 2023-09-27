CREATE OR REPLACE PROCEDURE SP_UPDATE_RESOURCES()
RETURNS VARIANT
LANGUAGE SQL
AS
DECLARE
    TABLES RESULTSET DEFAULT(select database_name, schema_name, table_name
                                , database_name||'.'||schema_name||'.'||table_name FQTN 
                                ,file_name
                            from resources);
    ret variant default '{}';   
BEGIN

    FOR tbl IN tables DO
        //drop all published files to internal stage
        execute immediate ( 'copy into @published_extracts/' ||
        IFNULL(tbl.file_name,tbl.table_name) || '.csv from ' ||
        tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = csv COMPRESSION = none NULL_IF=('''') EMPTY_FIELD_AS_NULL = FALSE FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
    END FOR;



//This makes another API call which stores the URL as a Resource
                                                                    
update resources
    set presigned_url = purl
    , date_updated = CURRENT_TIMESTAMP()
FROM (
        select get_presigned_url(@published_extracts, IFNULL(file_name,table_name) || '.csv',604800) purl
        ,database_name
        ,schema_name
        ,table_name
        from resources
    ) r
where r.database_name = resources.database_name
and r.schema_name = resources.schema_name
and r.table_name = resources.table_name;

insert into ckan_log 
select current_timestamp(),null,table_name 
from resource_stream;
    
exception
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    insert into ckan_log select localtimestamp(), 'ERROR', :err::string from resource_stream;
END;

