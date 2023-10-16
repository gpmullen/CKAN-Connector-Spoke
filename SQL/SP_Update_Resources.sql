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
    rec_limit_for_gzip int default 100000;
BEGIN

    FOR tbl IN tables DO
        let rs resultset := (execute immediate 'select count(*) cnt from ' || tbl.FQTN);
        let recs cursor for rs;
        FOR rec in recs DO //the is only one record to process,but we still need to use a looping syntax
            IF (rec.cnt > rec_limit_for_gzip) THEN //zip up the file if it's above the defined threshold
                //drop all published files to internal stage
                execute immediate ( 'copy into @published_extracts/' ||
                IFNULL(tbl.file_name,tbl.table_name) || '.csv.gz from ' ||
                tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = csv COMPRESSION = GZIP NULL_IF=('''') EMPTY_FIELD_AS_NULL = FALSE FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
            ELSE
                //drop all published files to internal stage
                execute immediate ( 'copy into @published_extracts/' ||
                IFNULL(tbl.file_name,tbl.table_name) || '.csv from ' ||
                tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = csv COMPRESSION = none NULL_IF=('''') EMPTY_FIELD_AS_NULL = FALSE FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
            END IF;
        END FOR;
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

