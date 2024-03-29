import streamlit as st
import json
from snowflake.snowpark import Session

try:
    #attempt file based credentials
    with open('creds.json') as f:
        data = json.load(f)
        username = data['username']
        password = data['password']
        account = data["account"]
except:
    username = ''
    password = ''
    account = ''
if 'getContext_set' not in st.session_state:
    st.session_state.getContext_set = False

def doSSO():
    st.session_state.isSSO = True
    getContext()

def getContext():
    if "txtUserName" in st.session_state: txtUserName=st.session_state["txtUserName"] 
    if "txtAccountLocator" in st.session_state: txtAccountLocator=st.session_state["txtAccountLocator"] 
    if "txtPassword" in st.session_state: txtPassword=st.session_state["txtPassword"] 

    if "isSSO" in st.session_state:
        CONNECTION_PARAMETERS = {
        'account': txtAccountLocator,
        'user': txtUserName,
        'password': '--',
        'loginTimeout': 10,
        'authenticator' : 'externalbrowser'
        }  

    if txtUserName and txtAccountLocator and txtPassword:
        CONNECTION_PARAMETERS = {
        'account': txtAccountLocator,
        'user': txtUserName,
        'password': txtPassword,
        'loginTimeout': 10,
        'authenticator' : 'username_password_mfa'
        }
    
    if 'CONNECTION_PARAMETERS' in locals():
        session = Session.builder.configs(CONNECTION_PARAMETERS).create()
        currentwh = session.sql('select current_warehouse()').collect()
        if currentwh[0][0] is None: #need a warehouse to check for roles assigned to the user
            fullroles = session.sql('SHOW ROLES').collect() #get all the roles in Snowflake
            roles = [row[1] for row in fullroles]
            st.info('All roles displayed because a Warehouse was not selected. Set Context with a WH and Get Context again for a filtered list of roles associated to your user.')
        else: 
            roles = session.sql('select value::STRING ROLE from table(flatten(input => parse_json(current_available_roles())))').to_pandas()
            
        whs = session.sql('SHOW WAREHOUSES').collect()
        fulldbs = session.sql('SHOW DATABASES').collect()
        dbs = [db[1] for db in fulldbs]
        schemas = session.sql('SHOW SCHEMAS').collect()
        
        st.session_state.roles = roles
        st.session_state.whs = whs
        st.session_state.dbs = dbs
        st.session_state.schemas = schemas
        
        session.close()
        st.session_state.connection_parameters = CONNECTION_PARAMETERS
        st.session_state.getContext_set = True
    else:
        if not txtUserName:
            st.info('User name is empty')
        if not txtPassword:
            st.info('Password is empty')
        if not txtAccountLocator:
            st.info('Account Locator is empty')

def getListValues(key):
    if key not in st.session_state:
        return []
    else:
        return st.session_state[key]

def filterSchema(ddlDatabase):
    if ddlDatabase:
        lstSchemas= getListValues('schemas')
        return [row[1] for row in lstSchemas if row[4] == ddlDatabase]
    else:
        return []
def setContext(txtAccountLocator,txtUserName,txtPassword,ddlSchema, ddlDatabase,ddlWarehouse,ddlRoles):
    CONNECTION_PARAMETERS = st.session_state.connection_parameters
    CONNECTION_PARAMETERS['schema'] = ddlSchema
    CONNECTION_PARAMETERS['database'] = ddlDatabase
    st.session_state.connection_parameters = CONNECTION_PARAMETERS
    st.session_state.context_set = True
    st.success('Saved!', icon="✅")

def getRoles(wh):
    st.session_state.isWarehouseSet = True
    CONNECTION_PARAMETERS = st.session_state.connection_parameters
    CONNECTION_PARAMETERS['warehouse'] = wh        
    st.session_state.connection_parameters = CONNECTION_PARAMETERS  
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    roles = session.sql('select value::STRING ROLE from table(flatten(input => parse_json(current_available_roles())))').to_pandas()
    st.session_state.roles = roles

def getDatabases(role):
    st.session_state.setRole = True
    CONNECTION_PARAMETERS = st.session_state.connection_parameters
    CONNECTION_PARAMETERS['role']= role
    st.session_state.connection_parameters = CONNECTION_PARAMETERS
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    fulldbs = session.sql('SHOW DATABASES').collect()
    dbs = [db[1] for db in fulldbs]
    schemas = session.sql('SHOW SCHEMAS').collect()
    st.session_state.schemas = schemas
    st.session_state.dbs = dbs

with st.form('GetContext'):
    txtAccountLocator = st.text_input('Account Locator',key='txtAccountLocator', value=account)
    txtUserName = st.text_input('User Name', value=username, key='txtUserName')
    btnSSO = st.form_submit_button('Single Sign-On', on_click=doSSO,type='secondary')
    txtPassword = st.text_input('Password', type="password", value=password, key='txtPassword')
    btnConnect = st.form_submit_button('Login', on_click=getContext,type='secondary')

if st.session_state.getContext_set:
    with st.container():
        st.info('This sets the context for the Control table')
        ddlWarehouse = st.selectbox('Warehouse', options=getListValues('whs'),help='Consider using the OPEN_DATA_VWH')        
        btnGetRoles = st.button('Get Roles', on_click=getRoles, args=[ddlWarehouse], type='secondary')
        if "isWarehouseSet" in st.session_state and st.session_state.isWarehouseSet:
            ddlRoles = st.selectbox('Roles', options=getListValues('roles'))
            btnGetDatbases = st.button('Get Databases', on_click=getDatabases, args=[ddlRoles], type='secondary')
        if "setRole" in st.session_state and st.session_state.setRole:
            ddlDatabase = st.selectbox('Database', options=getListValues('dbs'), help='Consider using the OPEN_DATA database')
            ddlSchema = st.selectbox('Schema', options=filterSchema(ddlDatabase))
            btnSet = st.button('Set Context', on_click=setContext, args=[txtAccountLocator,txtUserName,txtPassword,ddlSchema, ddlDatabase,ddlWarehouse,ddlRoles], type='primary')
