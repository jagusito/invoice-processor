from snowflake.snowpark import Session

def get_snowflake_session():
    """Get Snowflake session using your existing connection parameters"""
    connection_params = {
        "account": "CU83904.east-us-2.azure",
        "user": "BDA_USER", 
        "password": "Vyx(RO}gX/Z+:0<c",
        "role": "BDA_RW",
        "warehouse": "COMPUTE_WH",
        "database": "DEV_SC_BDA",
        "schema": "PUBLIC"
    }
    return Session.builder.configs(connection_params).create()