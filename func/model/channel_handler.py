import boto3
import os

from boto3.dynamodb.conditions import Key

def list_channels(event, context):
	dynamodb = boto3.resource('dynamodb', region_name=os.environ['REGION'])
	table = dynamodb.Table(os.environ['CHANNEL_TABLE'])
	
	scan_kwargs = {
		'FilterExpression': Key('channelSK').eq('GENERAL_INFO')
	}

	response = table.scan(**scan_kwargs)
	data = response.get('Items', [])

	print(data)

	return {
		'statusCode': 200,
		'body': 'channels listed'
	}
