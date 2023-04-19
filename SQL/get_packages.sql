    create or replace function CKAN_getpackage_request_translator(event object)
    returns object
    language javascript as
    '
    var query_string;

       let row = EVENT.body.data[0];
       query_string = "?fq=organization:" + row[1] + "&include_private=true&rows=100&start=0";

    return { "urlSuffix": query_string }
    ';    
    
    create or replace function CKAN_getpackage_response_translator(event object)
    returns object
    language javascript as
    '
    var responses = new Array(0);
    responses[0] = [0,EVENT.body.result.count]
    return { "body": { "data" : responses } };
    ';

create or replace external function package_search(owner_org varchar)
    returns variant
    request_translator=CKAN_package_search_request_translator
    response_translator=CKAN_package_search_response_translator
    api_integration = ckan_proxy_int
    as 'https://XXXX/package_search'
;