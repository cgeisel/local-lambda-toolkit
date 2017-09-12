import logging
import os
import yaml

from my_lambda_package.utility import Utility

import boto3
from botocore.exceptions import ClientError
import json
from croniter import croniter
from datetime import datetime

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _load_config(filename='config.yaml'):
    """Loads the configuration file."""
    with open(os.path.abspath(os.path.join(os.path.dirname(__file__), filename)), 'r') as f:
        config = yaml.load(f)
        logger.info('Loaded config: {0}'.format(config))
    return config

def get_plans(bucket, resource):
    logger.debug('Getting plans from bucket: {0}'.format(bucket))
    bucket = resource.Bucket(bucket)
    plans = {}
    for obj in bucket.objects.all():
        try:
            table_name = None
            file_name = obj.key
            scaling_plan = json.loads(obj.get()['Body'].read())
            table_name = scaling_plan['TableName']
            if obj.key != table_name:
                logger.error('File name does not match table name, skipping {0}'.format(file_name))
                continue
            if not scaling_plan['Enabled']:
                logger.info('Scaling plan for {0} not enabled, skipping'.format(table_name))
                continue
        except Exception as e:
            logger.error('Could not load scaling plan {0} ({1})'.format(file_name, e))

        plans[table_name] = scaling_plan

    return plans


def get_active_policies(now, scaling_plans):
    last_datetime = datetime.min
    active_policies = {}
    for table in scaling_plans:
        logger.info('Scaling plans for table: {0}'.format(table))
        logger.info('Now: {0}'.format(now))

        for policy in scaling_plans[table]['Policies']:
            cron = policy['Schedule']
            iter = croniter(cron, now)
            policy_start = iter.get_prev(datetime)

            logger.info('Checking policy: {0}'.format(policy['PolicyName']))
            logger.info('Policy active since: {0}'.format(policy_start))

            if policy_start > last_datetime:
                logger.info('Policy {0} started after {1}'.format(policy['PolicyName'], last_datetime))
                logger.info('Policy {0} now the current active policy'.format(policy['PolicyName']))

                last_datetime = policy_start
                active_policy = policy

                logger.info('ReadCapacityUnits: \tMin: {0} \tMax: {1} \tTargetValue: {2}'.format(active_policy['ReadCapacityUnits']['MinCapacity'], active_policy['ReadCapacityUnits']['MaxCapacity'], active_policy['ReadCapacityUnits']['TargetValue']))
                logger.info('WriteCapacityUnits: \tMin: {0} \tMax: {1} \tTargetValue: {2}'.format(active_policy['WriteCapacityUnits']['MinCapacity'], active_policy['WriteCapacityUnits']['MaxCapacity'], active_policy['WriteCapacityUnits']['TargetValue']))
            else:
                logger.info('Policy {0} did not start before {1}'.format(policy['PolicyName'], last_datetime))
                logger.info('Policy {0} still the current active policy'.format(active_policy['PolicyName']))

            active_policies[table] = active_policy

    return active_policies

def update_tables(active_policies, dynamodb, autoscaling):
    for table in active_policies:
        if is_active(table, dynamodb):
            response = autoscaling.describe_scaling_policies(
                PolicyNames=[
                    'DynamoDBReadCapacityUtilization:table/'+table,
                    'DynamoDBWriteCapacityUtilization:table/'+table
                ],
                ServiceNamespace='dynamodb',
                ResourceId='table/'+table
            )

            current_read_policy = filter(lambda policy: policy['PolicyName'] == 'DynamoDBReadCapacityUtilization:table/'+table, response['ScalingPolicies'])[0]
            if current_read_policy['TargetTrackingScalingPolicyConfiguration']['TargetValue'] != active_policies[table]['ReadCapacityUnits']['TargetValue']:
                update_read_policy(table, active_policies[table], autoscaling)

            current_write_policy = filter(lambda policy: policy['PolicyName'] == 'DynamoDBWriteCapacityUtilization:table/'+table, response['ScalingPolicies'])[0]
            if current_write_policy['TargetTrackingScalingPolicyConfiguration']['TargetValue'] != active_policies[table]['WriteCapacityUnits']['TargetValue']:
                update_write_policy(table, active_policies[table], autoscaling)

            response = autoscaling.describe_scalable_targets(
                ServiceNamespace='dynamodb',
                ResourceIds=[
                    'table/'+table
                ]
            )

            current_read_target = filter(lambda target: target['ScalableDimension'] == 'dynamodb:table:ReadCapacityUnits', response['ScalableTargets'])[0]
            if not (current_read_target['MinCapacity'] == active_policies[table]['ReadCapacityUnits']['MinCapacity'] or current_read_target['MaxCapacity'] == active_policies[table]['ReadCapacityUnits']['MaxCapacity']):
                update_read_target(table, active_policies[table], autoscaling)

            current_write_target = filter(lambda target: target['ScalableDimension'] == 'dynamodb:table:WriteCapacityUnits', response['ScalableTargets'])[0]
            if not (current_write_target['MinCapacity'] == active_policies[table]['WriteCapacityUnits']['MinCapacity'] or current_write_target['MaxCapacity'] == active_policies[table]['WriteCapacityUnits']['MaxCapacity']):
                update_write_target(table, active_policies[table], autoscaling)

def update_read_policy(table, policy, autoscaling):
    response = autoscaling.put_scaling_policy(
        PolicyName='DynamoDBReadCapacityUtilization:table/'+table,
        PolicyType='TargetTrackingScaling',
        ServiceNamespace='dynamodb',
        ResourceId='table/'+table,
        ScalableDimension='dynamodb:table:ReadCapacityUnits',
        TargetTrackingScalingPolicyConfiguration={
            'PredefinedMetricSpecification': {
                'PredefinedMetricType': 'DynamoDBReadCapacityUtilization'
            },
            'TargetValue': policy['ReadCapacityUnits']['TargetValue']
        }
    )
    print response

def update_write_policy(table, policy, autoscaling):
    response = autoscaling.put_scaling_policy(
        PolicyName='DynamoDBWriteCapacityUtilization:table/'+table,
        PolicyType='TargetTrackingScaling',
        ServiceNamespace='dynamodb',
        ResourceId='table/'+table,
        ScalableDimension='dynamodb:table:WriteCapacityUnits',
        TargetTrackingScalingPolicyConfiguration={
            'PredefinedMetricSpecification': {
                'PredefinedMetricType': 'DynamoDBWriteCapacityUtilization'
            },
            'TargetValue': policy['WriteCapacityUnits']['TargetValue']
        }
    )
    print response

def update_read_target(table, policy, autoscaling):
    response = autoscaling.register_scalable_target(
        ServiceNamespace='dynamodb',
        ResourceId='table/'+table,
        ScalableDimension='dynamodb:table:ReadCapacityUnits',
        MaxCapacity=policy['ReadCapacityUnits']['MaxCapacity'],
        MinCapacity=policy['ReadCapacityUnits']['MinCapacity']
    )
    print response

def update_write_target(table, policy, autoscaling):
    response = autoscaling.register_scalable_target(
        ServiceNamespace='dynamodb',
        ResourceId='table/'+table,
        ScalableDimension='dynamodb:table:WriteCapacityUnits',
        MaxCapacity=policy['WriteCapacityUnits']['MaxCapacity'],
        MinCapacity=policy['WriteCapacityUnits']['MinCapacity']
    )
    print response

def is_active(table, dynamodb):
    try:
        response = dynamodb.describe_table(
            TableName=table
        )

        if response['Table']['TableStatus'] != 'ACTIVE':
            logger.error('Table {0} is not in an ACTIVE state'.format(table))
            return False
        else:
            return True

    except ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 400:
            logger.error('Table {0} not found'.format(table))
        else:
            logger.error('Something went wrong')
        logger.error('Request ID {0}'.format(e.response['ResponseMetadata']['RequestId']))
        return False

def handler(event, context):
    """Entry point for the Lambda function."""
    config = _load_config()

    # Used to differentiate local vs Lambda.
    if bool(os.getenv('STUB')):
        logger.debug('$STUB set; likely running in development')
    else:
        logger.debug('No $STUB set; likely running in Lambda')

    logger.info('This is being invoked from AWS account: {0}'.format(
        Utility.aws_account_id()))

    bucket = config["s3_bucket"]
    s3 = boto3.resource('s3')
    plans = get_plans(bucket, s3)

    now = datetime.utcnow()
    active_policies = get_active_policies(now, plans)

    dynamodb = boto3.client('dynamodb')
    autoscaling = boto3.client('application-autoscaling')
    update_tables(active_policies, dynamodb, autoscaling)


if __name__ == '__main__':
    from my_lambda_package.localcontext import LocalContext
    handler(None, LocalContext())
