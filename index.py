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

    now = datetime.utcnow()
    last_datetime = datetime.min

    crontab_bucket = config["s3_bucket"]
    logger.debug('s3 bucket {0}'.format(crontab_bucket))
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(crontab_bucket)

    for obj in bucket.objects.all():
        table_config = json.loads(obj.get()['Body'].read())
        table_name = table_config['table']
        if obj.key != table_name:
            logger.error('File name does not match table name, skipping {0}'.format(obj.key))
            break

        if not table_config['enabled']:
            logger.info('Scaling plan for {0} not enabled, skipping'.format(table_name))
            break

        logger.info('Getting scaling policies for: \t{0}'.format(table_name))
        logger.info('Current time: \t{0}'.format(now))
        logger.debug('Policies: {0}'.format(table_config['policies']))

        active_policy = None
        for policy in table_config['policies']:
            cron = policy['cron']
            iter = croniter(cron, now)
            policy_start = iter.get_prev(datetime)
            logger.info('{0}: \t{1}'.format(policy['name'],policy_start))

            if policy_start > last_datetime:
                last_datetime = policy_start
                active_policy = policy

        logger.info('Using policy: \t{0}'.format(active_policy['name']))
        logger.info('\tReadCapacityUnits: \tMin: {0} \tMax: {1} \tTargetValue: {2}'.format(active_policy['ReadCapacityUnits']['MinCapacity'], active_policy['ReadCapacityUnits']['MaxCapacity'], active_policy['ReadCapacityUnits']['TargetValue']))
        logger.info('\tWriteCapacityUnits: \tMin: {0} \tMax: {1} \tTargetValue: {2}'.format(active_policy['WriteCapacityUnits']['MinCapacity'], active_policy['WriteCapacityUnits']['MaxCapacity'], active_policy['WriteCapacityUnits']['TargetValue']))

        # todo: check that table exists and is active
        dynamodb = boto3.client('dynamodb')
        try:
            response = dynamodb.describe_table(
                TableName=table_name
            )

            if response['Table']['TableStatus'] != 'ACTIVE':
                raise
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 400:
                logger.error('Table {0} not found'.format(table_name))
            else:
                logger.error('Something went wrong')
            logger.error('Request ID {0}'.format(e.response['ResponseMetadata']['RequestId']))
            raise

        # check existing target value and capacity
        autoscaling = boto3.client('application-autoscaling')
        existing_scaling_policies = autoscaling.describe_scaling_policies(
            PolicyNames=[
                'DynamoDBReadCapacityUtilization:table/'+table_name,
                'DynamoDBWriteCapacityUtilization:table/'+table_name
            ],
            ServiceNamespace='dynamodb',
            ResourceId='table/'+table_name
        )

        # print existing_scaling_policies
        update_read_policy = False
        update_write_policy = False
        for existing_policy in existing_scaling_policies['ScalingPolicies']:

            if (
                existing_policy['PolicyName'] == 'DynamoDBReadCapacityUtilization:table/'+table_name
                and active_policy['ReadCapacityUnits']['TargetValue'] != existing_policy['TargetTrackingScalingPolicyConfiguration']['TargetValue']
            ):
                update_read_policy = True

            if (
                existing_policy['PolicyName'] == 'DynamoDBWriteCapacityUtilization:table/'+table_name
                and active_policy['WriteCapacityUnits']['TargetValue'] != existing_policy['TargetTrackingScalingPolicyConfiguration']['TargetValue']
            ):
                update_write_policy = True

        existing_scalable_targets = autoscaling.describe_scalable_targets(
            ServiceNamespace='dynamodb',
            ResourceIds=[
                'table/'+table_name
            ]
        )

        # print existing_scalable_targets
        update_read_target = False
        update_write_target = False
        for existing_target in existing_scalable_targets['ScalableTargets']:

            if (
                existing_target['ScalableDimension'] == 'dynamodb:table:ReadCapacityUnits'
                and (
                    active_policy['ReadCapacityUnits']['MaxCapacity'] != existing_target['MaxCapacity']
                    or active_policy['ReadCapacityUnits']['MinCapacity'] != existing_target['MinCapacity']
                )
            ):
                update_read_target = True

            if (
                existing_target['ScalableDimension'] == 'dynamodb:table:WriteCapacityUnits'
                and (
                    active_policy['WriteCapacityUnits']['MaxCapacity'] != existing_target['MaxCapacity']
                    or active_policy['WriteCapacityUnits']['MinCapacity'] != existing_target['MinCapacity']
                )
            ):
                update_write_target = True


        # register scalable targets
        if (update_read_target):
            logger.info('read target: \tupdating')
            response = autoscaling.register_scalable_target(
                ServiceNamespace='dynamodb',
                ResourceId='table/'+table_name,
                ScalableDimension='dynamodb:table:ReadCapacityUnits',
                MaxCapacity=active_policy['ReadCapacityUnits']['MaxCapacity'],
                MinCapacity=active_policy['ReadCapacityUnits']['MinCapacity']
            )
            print response
        else:
            logger.info('read target: \tno change')

        if (update_write_target):
            logger.info('write target: \tupdating')
            response = autoscaling.register_scalable_target(
                ServiceNamespace='dynamodb',
                ResourceId='table/'+table_name,
                ScalableDimension='dynamodb:table:WriteCapacityUnits',
                MaxCapacity=active_policy['WriteCapacityUnits']['MaxCapacity'],
                MinCapacity=active_policy['WriteCapacityUnits']['MinCapacity']
            )
            print response
        else:
            logger.info('write target: \tno change')


        # put scaling policies
        if (update_read_policy):
            logger.info('read policy: \tupdating')
            response = autoscaling.put_scaling_policy(
                PolicyName='DynamoDBReadCapacityUtilization:table/'+table_name,
                PolicyType='TargetTrackingScaling',
                ServiceNamespace='dynamodb',
                ResourceId='table/'+table_name,
                ScalableDimension='dynamodb:table:ReadCapacityUnits',
                TargetTrackingScalingPolicyConfiguration={
                    'PredefinedMetricSpecification': {
                        'PredefinedMetricType': 'DynamoDBReadCapacityUtilization'
                    },
                    'TargetValue': active_policy['ReadCapacityUnits']['TargetValue']
                }
            )
            print response
        else:
            logger.info('read policy: \tno change')


        if (update_write_policy):
            logger.info('write policy: \tupdating')
            response = autoscaling.put_scaling_policy(
                PolicyName='DynamoDBWriteCapacityUtilization:table/'+table_name,
                PolicyType='TargetTrackingScaling',
                ServiceNamespace='dynamodb',
                ResourceId='table/'+table_name,
                ScalableDimension='dynamodb:table:WriteCapacityUnits',
                TargetTrackingScalingPolicyConfiguration={
                    'PredefinedMetricSpecification': {
                        'PredefinedMetricType': 'DynamoDBWriteCapacityUtilization'
                    },
                    'TargetValue': active_policy['WriteCapacityUnits']['TargetValue']
                }
            )
            print response
        else:
            logger.info('write policy: \tno change')



if __name__ == '__main__':
    from my_lambda_package.localcontext import LocalContext
    handler(None, LocalContext())
