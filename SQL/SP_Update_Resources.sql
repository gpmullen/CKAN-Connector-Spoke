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
    rec_limit_for_gzip int default 250000;
    ext string default '.csv';
BEGIN

    FOR tbl IN tables DO
        let rs resultset := (execute immediate 'select count(*) cnt from ' || tbl.FQTN);
        let recs cursor for rs;
        FOR rec in recs DO //the is only one record to process,but we still need to use a looping syntax
            IF (rec.CNT > rec_limit_for_gzip) THEN //zip up the file if it's above the defined threshold
                //drop all published files to internal stage
                ext := '.csv.gz';
                execute immediate ('copy into @published_extracts/' ||
                IFNULL(tbl.file_name,tbl.table_name) || :ext || ' from ' ||
                tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = csv COMPRESSION = GZIP NULL_IF=('''') EMPTY_FIELD_AS_NULL = FALSE FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
            ELSE
                //drop all published files to internal stage
                ext := '.csv';
                execute immediate ('copy into @published_extracts/' ||
                IFNULL(tbl.file_name,tbl.table_name) || :ext || ' from ' ||
                tbl.FQTN || ' SINGLE = TRUE MAX_FILE_SIZE=5368709120 OVERWRITE=TRUE HEADER=TRUE file_format = (TYPE = csv COMPRESSION = none NULL_IF=('''') EMPTY_FIELD_AS_NULL = FALSE FIELD_OPTIONALLY_ENCLOSED_BY=''\042'');');
            END IF;

            let sql string := 'UPDATE RESOURCES
            set presigned_url = get_presigned_url(@published_extracts, IFNULL(file_name,table_name) || \'' || :ext || '\',604800)
                , date_updated = CURRENT_TIMESTAMP()
            WHERE resources.database_name = \'' || tbl.database_name || '\'' ||
            ' and resources.schema_name = \'' || tbl.schema_name || '\'' ||
            ' and resources.table_name = \'' || tbl.table_name || '\'';
            execute immediate(:sql);

        END FOR;
    END FOR;

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
