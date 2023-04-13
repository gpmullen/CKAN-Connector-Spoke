CREATE OR REPLACE PROCEDURE SP_UPDATE_RESOURCES()
RETURNS VARIANT
LANGUAGE SQL
AS
DECLARE
    TABLES RESULTSET DEFAULT(select database_name, schema_name, table_name, database_name||'.'||schema_name||'.'||table_name FQTN 
    from resource_stream);
    ret variant default '{}';   
BEGIN

    FOR tbl IN tables DO
        //drop all published files to internal stage
        execute immediate ( 'copy into @published_extracts/' ||
        tbl.table_name || '.csv from ' ||
        tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = csv COMPRESSION = none FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
    END FOR;



//This makes another API call which stores the URL as a Resource
                                                                    
update resources
    set presigned_url = purl
FROM (
        select get_presigned_url(@published_extracts, table_name || '.csv',604800) purl
        from resources
    );

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

