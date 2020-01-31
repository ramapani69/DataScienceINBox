import snowflake.connector as sf

conn=sf.connect(user='ds_user01',password='November2019',account='jt51318.us-east-1')

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
    sql = 'use role {}'.format('INTEGRATION_ROLE')
    execute_query(conn, sql)

    sql = 'use database {}'.format('INTEGRATION_DB')
    execute_query(conn, sql)

    sql = 'use warehouse {}'.format('INTEGRATION_WH')
    execute_query(conn, sql)

    sql = 'use schema {}'.format('AWS_S3_STITCH_03')
    execute_query(conn, sql)

    try:
        sql = 'alter warehouse {} resume'.format('INTEGRATION_WH')
        execute_query(conn, sql)
    except:
        pass

    json_file = 'prediction.json'
    sql = "PUT file://" + json_file + " @cust_churn_stage auto_compress=true"
    execute_query(conn, sql)

    sql = 'copy into cust_churn_predict_json_v1(prediction) from @cust_churn_stage/prediction.json.gz file_format = (type = "json")' \
          'ON_ERROR = "ABORT_STATEMENT" '
    execute_query(conn, sql)

except Exception as e:
    print(e)
