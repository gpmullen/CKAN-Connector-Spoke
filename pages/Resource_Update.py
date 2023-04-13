import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StringType
from pages.Publish import getDatabases,getSchemas,getTables 


RESOURCE_TABLE = 'RESOURCES'
RESOURCE_DB='OPEN_DATA'
RESOURCE_SCHEMA='DEVELOPMENT'

def package_id_format(name):
    return st.session_state.packages.filter(col('PACKAGE_NAME')== name).select('PACKAGE_ID').cast(StringType)

def resource_id_format(resource_name):
    return st.session_state.packages.filter(col('RESOURCE_NAME')== resource_name).select('RESOURCE_ID').collect()

def getPackages(owner_org):
    if 'packages' not in st.session_state:
        session = Session.builder.configs(st.session_state.connection_parameters).create()
        st.session_state.packages = session.sql('''with cte as (select get_packages('{0}') data)
            select 
            packages.value:id::string PACKAGE_ID
            , packages.value:name::string PACKAGE_NAME
            , resources.value:id::string RESOURCE_ID
            , resources.value:name::string RESOURCE_NAME
            from cte,
            lateral flatten(input => cte.data:results) packages,
            lateral flatten(input => packages.value:resources) resources'''.format(owner_org))
    return st.session_state.packages.select('PACKAGE_NAME').distinct()

def setPackage():
    st.session_state.isPackageSet = True

def getResources(package_name):
    return st.session_state.packages.filter(col('PACKAGE_NAME')== package_name).select('RESOURCE_NAME')
    return set([row[3] for row in st.session_state.packages if row[1] == package_name])
    
def updateResource():
    if "ddlOwnerOrg" in st.session_state: ddlOwnerOrg=st.session_state["ddlOwnerOrg"] 
    if "ddlDatabaseToPublish" in st.session_state: ddlDatabaseToPublish=st.session_state["ddlDatabaseToPublish"] 
    if "ddlSchemaToPublish" in st.session_state: ddlSchemaToPublish=st.session_state["ddlSchemaToPublish"] 
    if "ddlTableToPublish" in st.session_state: ddlTableToPublish=st.session_state["ddlTableToPublish"] 
    package_id = package_id_format(ddlPackages)
    resource_id = resource_id_format(ddlResources)[0]
    st.write(package_id)
    session = Session.builder.configs(st.session_state.connection_parameters).create()        #Fully Qualified Table Name        
    dfControl = session.create_dataframe([[ddlOwnerOrg,ddlDatabaseToPublish,ddlSchemaToPublish,ddlTableToPublish,package_id, resource_id,None]])#,schema=["Notes","Access_Level","Contact_Name","Contact_Email","Rights","Accural_Periodicity","Tags","Owner_Org"])
    #insert
    
    #dfControl.write.mode("append").save_as_table("{0}.{1}.{2}".format(RESOURCE_DB,RESOURCE_SCHEMA,RESOURCE_TABLE))
    #session.sql("call SP_UPDATE_RESOURCES()").collect()
    session.close()
    st.success('Saved!', icon="✅")
    
if __name__ == "__main__":
    if 'connection_parameters' not in st.session_state:
        st.error('Set Context first!')
    else:
        ddlOwnerOrg = st.selectbox("Owner Org",('california-state-water-resources-control-board','sf-testing'), help='Required', key='ddlOwnerOrg')
        ddlPackages = st.selectbox("Packages", help='Required', key='ddlPackages', options=getPackages(ddlOwnerOrg), on_change=setPackage)
        if 'isPackageSet' in st.session_state:
            ddlResources = st.selectbox("Resources", help='Required', key='ddlResources', options=getResources(ddlPackages))
            ddlDatabaseToPublish = st.selectbox("Database", options=getDatabases(), help='Required', key='ddlDatabaseToPublish')
            ddlSchemaToPublish = st.selectbox("Schema", options=getSchemas(), help='Required', key='ddlSchemaToPublish')
            ddlTableToPublish = st.selectbox("Tables to Publish", options=getTables(), help='Required', key='ddlTableToPublish')
            btnPublish = st.button("Publish", on_click=updateResource, type='primary')
          
