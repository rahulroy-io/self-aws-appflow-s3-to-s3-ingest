#%% IMPORTS
#######################TASK00#######################
print ('STARTING JOB')

print ('STARTED TASK00 IMPORTS')

import sys
import boto3
from botocore.exceptions import ClientError, PaginationError
from concurrent.futures import ThreadPoolExecutor

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

source_bucket_prefix = 'applow-source/redemer'
source_bucket = 'rhl-temp'

target_bucket_prefix = 'applow-target/redemer'
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
            "Name_of_the_Political_Party"
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

flow_config = {
    'triggerConfig': trigger_config,
    'sourceFlowConfig': source_flow_config,
    'destinationFlowConfigList': destination_flow_config_list,
    'tasks': tasks
}

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
        
    def flow_exists(self):
        """
        Check if a flow with the given name exists.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        flow_name = self.flow_name
        try:
            self.client.describe_flow(flowName=flow_name)
        except self.client.exceptions.ResourceNotFoundException:
            print(f"Resource not found error while checking flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.InternalServerException as e:
            print(f"Internal server error while checking flow '{flow_name}': {e}")
            return False
        except Exception as e:
            print (f"Error checking if flow '{flow_name}' exists: {e}")
            #raise (f"Error checking if flow '{flow_name}' exists: {e}")
            return False
        else:
            return True

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
        except self.client.exceptions.ResourceNotFoundException as e:
            print(f"Flow '{flow_name}' not found: {e}")
            return False
        except self.client.exceptions.InternalServerException as e:
            print(f"Internal server error while starting flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ServiceQuotaExceededException as e:
            print(f"Service quota exceeded for flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConflictException as e:
            print(f"Conflict occurred while starting flow '{flow_name}': {e}")
            return False
        except Exception as e:
            print (f"Error in starting flow '{flow_name}': {e}")
            #raise (f"Error in starting flow '{flow_name}': {e}")
            return False
        else:
            return True
    
    def get_execution_status(self):
        """
        Get the status of a specific flow execution.

        :param flow_name: The name of the flow.
        :param execution_id: The execution ID to check.
        :return: A tuple containing the execution status and the execution result (if available).
        """
        flow_name = self.flow_name
        execution_status = False
        if self.execution_id==None:
            print (f"{flow_name} not yet started")
            raise Exception(f"{flow_name} not yet started")
        else:
            execution_id = self.execution_id
        try:
            response = appflow_client.describe_flow_execution_records(flowName=flow_name, maxResults=1)
            flow_executions = response.get('flowExecutions')
            for flow_execution in flow_executions:
                if flow_execution.get('executionId')==execution_id:
                    print (flow_execution)
                    execution_status = flow_execution.get('executionStatus')

            while 'nextToken' in response:
                next_token = response.get('nextToken')
                response = appflow_client.describe_flow_execution_records(flowName=flow_name, maxResults=1, nextToken = next_token)
                flow_executions = response.get('flowExecutions')
                for flow_execution in flow_executions:
                    if flow_execution.get('executionId')==execution_id:
                        print (flow_execution)
                        execution_status = flow_execution.get('executionStatus')
        except self.client.exceptions.ValidationException as e:
            print(f"Validation error while fetching status for execution '{execution_id}': {e}")
        except self.client.exceptions.ResourceNotFoundException as e:
            print(f"Flow '{flow_name}' or execution '{execution_id}' not found: {e}")
        except self.client.exceptions.InternalServerException as e:
            print(f"Internal server error while fetching execution status: {e}")
        except Exception as e:
            print(f"Error retrieving execution status for '{execution_id}': {e}")
        finally:
            if (execution_status):
                self.execution_status = execution_status
                return execution_status
            else:
                print(f"Execution ID '{execution_id}' not found for flow '{flow_name}'.")
                #raise Exception((f"Execution ID '{execution_id}' not found for flow '{flow_name}'."))
                execution_status = 'UnKnown'
                self.execution_status = execution_status
                return execution_status

    def create_flow(self, flow_config):
        """
        Create a new AppFlow with the specified configuration.
        :param flow_name: Name of the new AppFlow.
        :param flow_config: Dictionary containing the flow configuration.
        :return: Flow creation status.
        """
        flow_name = self.flow_name
        try:
            response = self.client.create_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"Flow '{flow_name}' created successfully.")
            print (response)
        except self.client.exceptions.ValidationException as e:
            print(f"Validation error while creating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.InternalServerException as e:
            print(f"Internal server error while creating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ResourceNotFoundException as e:
            print(f"Resource not found for flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ServiceQuotaExceededException as e:
            print(f"Service quota exceeded while creating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConflictException as e:
            print(f"Conflict error while creating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConnectorAuthenticationException as e:
            print(f"Connector authentication error for flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConnectorServerException as e:
            print(f"Connector server error while creating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.AccessDeniedException as e:
            print(f"Access denied while creating flow '{flow_name}': {e}")
            return False
        except Exception as e:
            raise Exception(f"Unexpected error while creating flow '{flow_name}': {e}")
        else:
            return True
        
    def update_flow(self, flow_config):
        """
        Update an existing flow with the given configuration.

        :param flow_name: The name of the flow to update.
        :param flow_config: A dictionary containing the updated flow configuration.
        """
        flow_name = self.flow_name
        try:
            response = self.client.update_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"Flow '{flow_name}' updated successfully.")
            print (response)
        except self.client.exceptions.ResourceNotFoundException as e:
            print(f"Flow '{flow_name}' not found: {e}")
            return False
        except self.client.exceptions.ServiceQuotaExceededException as e:
            print(f"Service quota exceeded while updating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConflictException as e:
            print(f"Conflict occurred while updating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConnectorAuthenticationException as e:
            print(f"Connector authentication error for flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.ConnectorServerException as e:
            print(f"Connector server error for flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.InternalServerException as e:
            print(f"Internal server error while updating flow '{flow_name}': {e}")
            return False
        except self.client.exceptions.AccessDeniedException as e:
            print(f"Access denied while updating flow '{flow_name}': {e}")
            return False
        except Exception as e:
            raise (f"Failed to update flow '{flow_name}': {e}")
        else:
            return True
        
    def create_or_update_flow(self, flow_config):
        """
        Create a new flow or update an existing flow.

        :param flow_name: The name of the flow to create or update.
        :param flow_config: A dictionary containing the flow configuration.
        :return: True if the flow was created or updated successfully, False otherwise.
        """
        flow_name = self.flow_name
        try:
            if self.flow_exists():
                print(f"Flow '{flow_name}' exists. Updating...")
                self.update_flow(flow_config)
                print(f"Flow '{flow_name}' updated successfully.")
            else:
                print(f"Flow '{flow_name}' does not exist. Creating...")
                self.create_flow(flow_config)
                print(f"Flow '{flow_name}' created successfully.")
        except ClientError as e:
            raise (f"Failed to create or update flow '{flow_name}': {e}")
        else:
            return True
        
    def in_terminal_sate(self):
        """
        Check if a flow execution has reached terminal state or not.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        flow_name = self.flow_name
        try:
            flow_status = self.get_execution_status()
        except Exception as e:
            print (f"Error checking flow '{flow_name}' status exists: {e}")
            #raise (f"Error checking if flow '{flow_name}' exists: {e}")
            return False
        else:
            if flow_status in ['Successful', 'Error', 'CancelStarted', 'Canceled']:
                return True
            else:
                return False


#%% 
# Execution:

    

#################

