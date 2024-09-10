#%% IMPORTS
#######################TASK00#######################
print ('STARTING JOB')

print ('STARTED TASK00 IMPORTS')

import sys
import boto3
from botocore.exceptions import ClientError, PaginationError
from concurrent.futures import ThreadPoolExecutor, as_completed

import time
from datetime import datetime as dt, timedelta

import json

from pyspark.sql import SparkSession, types as T, functions as F
from pyspark import SparkConf

conf = SparkConf().setAppName("learn")
conf.set('spark.jars.packages', 'io.delta:delta-core_2.12:2.1.0')
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
print ('########TASK00-IMPORTS-COMPLETED-SUCCESSFULLY########')

#%% PARAMETERS
#######################TASK01#######################
print ("STARTED TASK01 PARAMETERS INITIALIZING")

appflow_client = boto3.client('appflow')
s3_client = boto3.resource('s3')

JOB_NAME = 'job_self_aws_appflow_s3_to_s3_ingest'
START_DATE = ''
END_DATE = ''
MAX_RETRIES = 3 
FILTER_FIELD = ''
JOB_RUN_ID = '1234'
APP_BUCKET = 'rhl-temp'

appflow_bucket = APP_BUCKET
appflow_target_path = 's3://{appflow_bucket}/{flow_name}/{execution_id}'
appflow_target_path = 'rhl-temp/applow-target/test-flow/schemaVersion_2/'



client = JOB_NAME.split('_')[1]
domain = JOB_NAME.split('_')[2]
entity = JOB_NAME.split('_')[3]
source = JOB_NAME.split('_')[4]
target = JOB_NAME.split('_')[6]

flow_name = f'applow-{client}-{domain}-{entity}-{source}-{target}'

print (f"JOB_NAME :{JOB_NAME}")
print (f"START_DATE :{START_DATE}")
print (f"END_DATE :{END_DATE}")
print (f"MAX_RETRIES :{MAX_RETRIES}")
print (f"FILTER_FIELD :{FILTER_FIELD}")
print (f"JOB_RUN_ID :{JOB_RUN_ID}")
print (f"APP_BUCKET :{APP_BUCKET}")

print (f"client :{client}")
print (f"domain :{domain}")
print (f"entity :{entity}")
print (f"source :{source}")
print (f"target :{target}")
print (f"flow_name :{flow_name}")

print ('########TASK01-PARAMETERS-INITIALIZED-COMPLETED-SUCCESSFULLY########')

#%% UDFs and Class Defination
print ("STARTED TASK03 UDFs AND Class Defination")
class AppFlowManager:
    def __init__(self, appflow_client):
        """
        Initialize the AppFlowManager with a Boto3 client for AppFlow.

        :param region_name: AWS region name (e.g., 'us-east-1'). If None, uses the default region.
        """
        self.client = appflow_client

    def start_flow(self, flow_name, client_token=None):
        """
        Start an AppFlow flow execution.

        :param flow_name: The name of the flow to start.
        :param client_token: (Optional) A unique, case-sensitive string to ensure idempotency.
        :return: The execution ID of the started flow.
        """
        try:
            response = self.client.start_flow(
                flowName=flow_name,
                clientToken=client_token  # Optional, can be omitted
            )
            execution_id = response.get('executionId')
            print(f"Started flow '{flow_name}'. Execution ID: {execution_id}")
            return execution_id
        except ClientError as e:
            print(f"Failed to start flow '{flow_name}': {e}")
            return None

    def get_execution_status(self, flow_name, execution_id):
        """
        Get the status of a specific flow execution.

        :param flow_name: The name of the flow.
        :param execution_id: The execution ID to check.
        :return: A tuple containing the execution status and the execution result (if available).
        """
        try:
            paginator = self.client.get_paginator('describe_flow_execution_records')
            page_iterator = paginator.paginate(flowName=flow_name)

            for page in page_iterator:
                for record in page.get('flowExecutions', []):
                    if record.get('executionId') == execution_id:
                        status = record.get('executionStatus')
                        result = record.get('executionResult')
                        print(f"Execution ID '{execution_id}' Status: {status}")
                        return status, result
            print(f"Execution ID '{execution_id}' not found for flow '{flow_name}'.")
            return None, None
        except (ClientError, PaginationError) as e:
            print(f"Error retrieving execution status for '{execution_id}': {e}")
            return None, None

    def create_or_update_flow(self, flow_name, flow_config):
        """
        Create a new flow or update an existing flow.

        :param flow_name: The name of the flow to create or update.
        :param flow_config: A dictionary containing the flow configuration.
        :return: True if the flow was created or updated successfully, False otherwise.
        """
        try:
            if self.flow_exists(flow_name):
                print(f"Flow '{flow_name}' exists. Updating...")
                self.update_flow(flow_name, flow_config)
                print(f"Flow '{flow_name}' updated successfully.")
            else:
                print(f"Flow '{flow_name}' does not exist. Creating...")
                self.create_flow(flow_name, flow_config)
                print(f"Flow '{flow_name}' created successfully.")
            return True
        except ClientError as e:
            print(f"Failed to create or update flow '{flow_name}': {e}")
            return False

    def flow_exists(self, flow_name):
        """
        Check if a flow with the given name exists.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        try:
            self.client.describe_flow(flowName=flow_name)
            return True
        except self.client.exceptions.ResourceNotFoundException:
            return False
        except ClientError as e:
            print(f"Error checking if flow '{flow_name}' exists: {e}")
            raise

    def create_flow(self, flow_name, flow_config):
        """
        Create a new flow with the given configuration.

        :param flow_name: The name of the flow to create.
        :param flow_config: A dictionary containing the flow configuration.
        """
        try:
            self.client.create_flow(
                flowName=flow_name,
                triggerConfig=flow_config['triggerConfig'],
                sourceFlowConfig=flow_config['sourceFlowConfig'],
                destinationFlowConfigList=flow_config['destinationFlowConfigList'],
                tasks=flow_config['tasks'],
                description=flow_config.get('description', ''),
                tags=flow_config.get('tags', {})
            )
        except ClientError as e:
            print(f"Failed to create flow '{flow_name}': {e}")
            raise

    def update_flow(self, flow_name, flow_config):
        """
        Update an existing flow with the given configuration.

        :param flow_name: The name of the flow to update.
        :param flow_config: A dictionary containing the updated flow configuration.
        """
        try:
            self.client.update_flow(
                flowName=flow_name,
                triggerConfig=flow_config['triggerConfig'],
                sourceFlowConfig=flow_config['sourceFlowConfig'],
                destinationFlowConfigList=flow_config['destinationFlowConfigList'],
                tasks=flow_config['tasks'],
                description=flow_config.get('description', '')
            )
        except ClientError as e:
            print(f"Failed to update flow '{flow_name}': {e}")
            raise

    def execute_async_flows(self, flow_name, date_ranges, max_concurrent=10):
        """
        Execute multiple flows asynchronously with a ThreadPoolExecutor.

        :param flow_name: The name of the flow to start.
        :param date_ranges: List of date range tuples (start_date, end_date).
        :param max_concurrent: Maximum number of concurrent executions.
        """
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            future_to_date_range = {
                executor.submit(self.start_flow, flow_name, client_token=f'{start_date}_{end_date}'): (start_date, end_date)
                for start_date, end_date in date_ranges
            }

            for future in as_completed(future_to_date_range):
                start_date, end_date = future_to_date_range[future]
                try:
                    execution_id = future.result()
                    if execution_id:
                        status, result = self.get_execution_status(flow_name, execution_id)
                        print(f"Flow execution for date range ({start_date} to {end_date}) status: {status}")
                        if result:
                            print(f"Execution result for date range ({start_date} to {end_date}): {result}")
                except Exception as e:
                    print(f"Error executing flow for date range ({start_date} to {end_date}): {e}")

#%% 
# Example usage:

if __name__ == "__main__":
    # Initialize the AppFlowManager
    appflow_manager = AppFlowManager(region_name='us-east-1')

    # Define the flow configuration
    flow_config = {}

    # Create or update the flow
    appflow_manager.create_or_update_flow(flow_name, flow_config)

    # Define date ranges for historical data
    from datetime import datetime, timedelta
    date_ranges = [
        (start_date.strftime('%Y-%m-%d'), (start_date + timedelta(days=6)).strftime('%Y-%m-%d'))
        for start_date in (datetime(2023, 1, 1) + timedelta(weeks=n) for n in range(52))
    ]

    # Execute the flows asynchronously
    appflow_manager.execute_async_flows(flow_name, date_ranges, max_concurrent=10)
    
    

#################
#%% 
import boto3
from botocore.exceptions import ClientError

source_bucket_prefix = 'applow-source/redemer'
source_bucket = 'rhl-temp'

target_bucket_prefix = 'applow-source/redemer'
target_bucket = 'rhl-temp'

source_flow_config = {
    "connectorType": "S3",
    "sourceConnectorProperties": {
        "S3": {
            "bucketName": source_bucket,
            "bucketPrefix": source_bucket_prefix,
            "s3InputFormatConfig": {
                "s3InputFileType": "CSV"
            }
        }
    }
}

destination_flow_config_list = [
    {
        "connectorType": "S3",
        "destinationConnectorProperties": {
            "S3": {
                "bucketName": target_bucket,
                "bucketPrefix": target_bucket_prefix,
                "s3OutputFormatConfig": {
                    "fileType": "JSON",
                    "prefixConfig": {},
                    "aggregationConfig": {
                        "aggregationType": "SingleFile"
                    }
                }
            }
        }
    }
]
tasks = [
    {
        "sourceFields": [
            "Sr_No",
            "Date_of_Encashment",
            "Name_of_the_Political_Party",
            "Account_no._of_Political_Party"
        ],
        "connectorOperator": {
            "S3": "PROJECTION"
        },
        "taskType": "Filter",
        "taskProperties": {}
    },
    {
        "sourceFields": [],
        "connectorOperator": {
            "S3": "NO_OP"
        },
        "taskType": "Map_all"
    }
]

trigger_config = {
    'triggerType':'OnDemand'
}

class AppFlowWrapper:
    def __init__(self, flow_name, flow_client):
        """
        Initialize the AppFlowWrapper with a Boto3 AppFlow client supplied externally.

        :param flow_client: The AppFlow client object supplied during initialization.
        :param flow_name: The AppFlow name object supplied during initialization.
        """
        self.flow_name = flow_name
        self.client = flow_client
        self.execution_id = None
        
    def flow_exists(self, flow_name=None):
        """
        Check if a flow with the given name exists.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        if flow_name==None: flow_name = self.flow_name
        try:
            self.client.describe_flow(flowName=flow_name)
            return True
        except self.client.exceptions.ResourceNotFoundException:
            return False
        except ClientError as e:
            print(f"Error checking if flow '{flow_name}' exists: {e}")
            raise (f"Error checking if flow '{flow_name}' exists: {e}")

    def start_flow(self):
        """
        Start an AppFlow flow execution.

        :param flow_name: The name of the flow to start.
        :param client_token: (Optional) A unique, case-sensitive string to ensure idempotency.
        :return: The execution ID of the started flow, or None in case of error.
        """
        try:
            flow_name = self.flow_name
            response = self.client.start_flow(
                flowName=flow_name
            )
            execution_id = response.get('executionId')
            print(f"Started flow '{flow_name}'. Execution ID: {execution_id}")
            self.execution_id = execution_id
            self.execution_start_time = time.time()
            return True

        except self.client.exceptions.ResourceNotFoundException as e:
            print(f"Flow '{flow_name}' not found: {e}")
        except self.client.exceptions.InternalServerException as e:
            print(f"Internal server error while starting flow '{flow_name}': {e}")
        except self.client.exceptions.ServiceQuotaExceededException as e:
            print(f"Service quota exceeded for flow '{flow_name}': {e}")
        except self.client.exceptions.ConflictException as e:
            print(f"Conflict occurred while starting flow '{flow_name}': {e}")
        except ClientError as e:
            print(f"Failed to start flow '{flow_name}': {e}")
        return False
    
    def get_execution_status(self, flow_name=None, execution_id=None):
        """
        Get the status of a specific flow execution.

        :param flow_name: The name of the flow.
        :param execution_id: The execution ID to check.
        :return: A tuple containing the execution status and the execution result (if available).
        """
        if flow_name == None: flow_name = self.flow_name
        if self.execution_id==None:
            print (f"{flow_name} not yet started")
            raise Exception(f"{flow_name} not yet started")
        try:
            if execution_id == None: execution_id = self.execution_id
            response = appflow_client.describe_flow_execution_records(flowName=flow_name, maxResults=1)
            flow_executions = response.get('flowExecutions')
            for flow_execution in flow_executions:
                if flow_execution.get('executionId')==execution_id:
                    print (flow_execution)
                    execution_status = flow_execution.get('executionStatus')
                    self.execution_status = execution_status
                    return execution_status

            while 'nextToken' in response:
                next_token = response.get('nextToken')
                response = appflow_client.describe_flow_execution_records(flowName=flow_name, maxResults=1, nextToken = next_token)
                flow_executions = response.get('flowExecutions')
                for flow_execution in flow_executions:
                    if flow_execution.get('executionId')==execution_id:
                        print (flow_execution)
                        execution_status = flow_execution.get('executionStatus')
                        self.execution_status = execution_status
                        return execution_status

            print(f"Execution ID '{execution_id}' not found for flow '{flow_name}'.")
            return None, None
        except ClientError as ce:
            execution_status = 'Unknown Status'
            self.execution_status = execution_status
            print(f"Error retrieving execution status for '{execution_id}': {ce}")
            return execution_status
        except Exception as e:
            print(f"Error retrieving execution status for '{execution_id}': {e}")

    def create_flow(self, flow_name, flow_config):
        """
        Create a new AppFlow with the specified configuration.
        :param flow_name: Name of the new AppFlow.
        :param flow_config: Dictionary containing the flow configuration.
        :return: Flow creation status.
        """
        try:
            response = self.client.create_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"Flow '{flow_name}' created successfully.")
            return response
        except ClientError as e:
            print(f"Error creating flow '{flow_name}': {str(e)}")
            return None
