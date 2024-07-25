import os
import json
import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
table_inference = os.environ["table_inference"]

def lambda_handler(event, context):
    print(event)
    try:
        response = create_table(table_inference)
    except Exception as table_exception:
        logger.error("Error in %s creating the table",table_exception)
        exception_text = str(table_exception)
        logger.info(exception_text)
    input_data = {}
    # uuid = event["uuid"]
    uuid = event[0]["uuid"]
    complaint = event[0]["complaint"]
    # byte_code = None
    # # Iterate through the list of dictionaries
    # for item in event:
    #     # Check if 'byte_code' key exists in the dictionary
    #     if 'byte_code' in item:
    #         byte_code = item['byte_code']
    #         break
    for item in event:
        if "level" in item:
            input_data["level"] = item["level"]
        if "category" in item:
            input_data["category"] = item["category"]
    print(input_data)
    # input_data = {
    #     'level': {
    #         "2": 0.99,
    #         "1": 0.01,
    #         "3": 0
    #     },
    #     'category': {
    #         "Needle bent": 0.97,
    #         "Leaking unspecified": 0.93,
    #         "Miscellaneous Sub-Category": 0.47
    #     }
    # }
    # # fetch the json mapping file from s3
    # bucket_name = 'qms-complaints-file-storage'
    # key = 'crl_mapping.json'
    # response = s3.get_object(Bucket=bucket_name, Key=key)
    # json_content = response['Body'].read().decode('utf-8')
    table_name = 'mq-qms-lookup-mapping'
    items = scan_dynamodb_table(table_name)
    # print(type(items))
    json_data = json.dumps(items)
    print(json_data)
    mapping_data = json.loads(json_data)
    # print(mapp)
    #mapp = {'Leaking unspecified': 'Leaking unspecified - CSC', 'Needle bent': 'Needle bent - CSC', 'Device not working': 'Device not working - CSC', 'Clicks': 'Clicks - CSC', 'Device defective': 'Device defective - CSC', 'Needle broken': 'Needle broken - ASC', 'Injection incomplete': 'Injection incomplete - CSC', 'Upside down injection': 'Upside down injection - CSC', 'Device difficult to unlock': 'Device difficult to unlock - CSC', 'Device activated before placement on skin': 'Device activated before placement on skin - CSC', 'Leaking when pulling off base cap': 'Leaking when pulling off base cap - CSC', 'Leaking after injection from device': 'Leaking after injection from device - CSC', 'Leaking after injection from injection site': 'Leaking after injection from injection site - CSC', 'Dose confirmation': 'Dose confirmation - CSC', 'Injection button difficult to press': 'Injection button difficult to press - CSC', 'Base cap difficult to remove': 'Base cap difficult to remove - CSC', 'Base cap replacement': 'Base cap replacement – CSC', 'Device activated with base cap attached': 'Device activated with base cap attached - ASC', 'Injection takes too long': 'Injection takes too long - CSC', 'Air bubble': 'Air bubble - CSC', 'Injection is too fast': 'Injection is too fast  - CSC', 'Missing primary container of drug product/diluent/device': 'Missing primary container of drug product/diluent/device - CSC', 'Carton - MINOR Damage/Defect': 'Carton - MINOR Damage/Defect - CSC', 'Drug residue': 'Drug residue - CSC', 'Pen was used from package': 'Pen was used from package - CSC', 'Needle not fully extended': 'Needle not fully extended - CSC', 'Device activated before pressing button': 'Device activated before pressing button - CSC', 'Device was not locked when removed from package': 'Device was not locked when removed from package - CSC', 'Device activated when removed from carton': 'Device activated when removed from carton - CSC', 'Foaming after actuation': 'Foaming after actuation - CSC', 'Miscellaneous Sub-Category': 'Miscellaneous Sub-Category'}
    # mapp = json.loads(json_content)
    # print(mapp)
    mapp = {item['crl_mapping'].split(' : ')[0]: item['crl_mapping'].split(' : ')[1] for item in mapping_data}
    # print(f"step 1: get the json object from s3 bucket {mapp}")
    crl = {}
    for k,v in input_data['category'].items():
        if k in mapp:
            crl[mapp[k]] = v
        else:
            crl['Unassigned CRL'] = v
    print(f"step 2: perform crl mapping {crl}")
    result = input_data
    result['CRL Title'] = crl
    print(f"step 3: append the final output result {result}")
    try:
        response = insert_data(table_inference ,uuid, result#, byte_code
        )
        logger.info("item has been created")
    except Exception as insert_exception:
        logger.error("Error in %s saving the raw data",insert_exception)
        exception_text = str(insert_exception)
        logger.info(exception_text)
        return {
            'statusCode': 503,
            'body': json.dumps(str(exception_text))
        }
    priority = result
    priority['uuid'] = uuid
    priority['complaint'] = complaint
    return {
        'statusCode': 200,
        'body': json.dumps(priority)
    }

def scan_dynamodb_table(table_name):
    # Initialize a session using Amazon DynamoDB
    dynamodb = boto3.resource('dynamodb')

    # Initialize the table
    table = dynamodb.Table(table_name)

    # Scan the table and get all items
    response = table.scan()

    # Get the items from the response
    data = response['Items']

    # Continue to get all items if the scan is paginated
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return data
  

def create_table(table_name):
    # Create DynamoDB table
    try:
        create_table_response = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'uuid',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'uuid',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        logger.info(f"Table {table_name} created successfully.")
        return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB table created')
        }
    except dynamodb_client.exceptions.ResourceInUseException:
        logger.info(f"Table {table_name} already exists.")
        
def insert_data(table_name, item_uuid, result#, byte_code
):
    # Put item into table
    try:
        put_item_response = dynamodb_client.put_item(
            TableName= table_name,
            Item={
                'uuid': {'S': str(item_uuid)},
                'results': {'S': str(result)}#,
                #'byte_code': {'S': str(byte_code)},
            })
        logger.info(f"Item with uuid {item_uuid} added successfully.")
    except Exception as e:
        logger.info(f"Error putting item: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB item setup completed.')
    }