
CREATE OR REPLACE TASK CKAN_SPOKE_URL_REFRESH
warehouse ='OPEN_DATA_VWH'
SCHEDULE = '1440 MINUTE' //1 DAY
AS
BEGIN
   update control_spoke
    set presigned_url = purl
FROM (
        select get_presigned_url(@published_extracts, table_name || '.csv',604800) purl
        from control_spoke
    );

    insert into ckan_log select localtimestamp(), 'SCHEDULED REFRESH', 'COMPLETE';
exception
  when other then
    let err := object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
    insert into ckan_log select localtimestamp(), 'ERROR', :err::string;    
END;

alter task ckan_url_refresh resume;

execute task CKAN_URL_REFRESH;