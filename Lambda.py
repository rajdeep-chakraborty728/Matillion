import json
import boto3
import os
import sys
import traceback

sqs = boto3.resource('sqs');
queue = sqs.get_queue_by_name(QueueName=os.environ.get('QUEUE_NAME'));

sns_arn =  os.environ.get('SNS_TOPIC_NAME');
sns_region =  os.environ.get('SNS_REGION');
sns_client=boto3.client('sns');

def lambda_handler (event,context):

    try:

        for record in event['Records']:

            varInputFileName=record.get('s3').get('object').get('key');

            sqs_msg = {
                "group":       os.environ.get('MAT_GROUP')
               ,"project":     os.environ.get('MAT_PROJECT')
               ,"version":     os.environ.get('MAT_VRSN')
               ,"environment": os.environ.get('MAT_ENV')
               ,"job":         os.environ.get('MAT_JOB')
               ,"variables":  {
                    "JobVarInputFile": varInputFileName,
                    "JobVarInputBucket":  os.environ.get('INP_BUCKET'),
                    "JobVarArchiveBucket":  os.environ.get('OUT_BUCKET'),
                    "JobVarArchiveDir":  os.environ.get('OUT_DIR')
                    }
            }

            queue.send_message(MessageBody=json.dumps(sqs_msg));

        vSubject='Marketo Openprise Event Trigger Successfull';
        vMessage='Lambda Function '+os.environ.get('AWS_LAMBDA_FUNCTION_NAME')+' Invoked Matillion Job '+os.environ.get('MAT_JOB')+' Succesfully';

        sns_client.publish(
            TargetArn=sns_arn,
            Subject=vSubject,
            Message=vMessage
            );

        return event

    except:

        vSubject='Marketo Openprise Event Trigger Failure';
        vMessage='Lambda Function '+os.environ.get('AWS_LAMBDA_FUNCTION_NAME')+' Invokation of Matillion Job '+os.environ.get('MAT_JOB')+' Failed '+CHAR(10)+CHAR(10)+traceback.format_exc();

        sns_client.publish(
            TargetArn=sns_arn,
            Subject=vSubject,
            Message=vMessage
            );
