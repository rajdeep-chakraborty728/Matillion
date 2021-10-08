#####################################################################
### Code Is Compatible With Matillion Variables ###
#####################################################################


import requests as rqst #requests Module To Be Installed in Server
import json
import base64
import datetime
import time
import csv
from io import StringIO
import pandas as pd #Pandas Module To Be Installed in Server
from sqlalchemy import create_engine #SQLAlchemy Connector Module To Be Installed in Server
from snowflake.sqlalchemy import URL #snowflake.sqlalchemy Module To Be Installed in Server
import sys #To Trap Unhandled Exception
import traceback #To Trap Unhandled Exception

####################################################################################
#############Testing Purpose. Will be removed during Productionisation##############
####################################################################################
#pd.options.display.float_format = '{:.2f}'.format

def CreateDBConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole):

	####################################################################################
	#############Creates a Connection Instance to the Snowflake Server##################
	####################################################################################

	global vGlobalErrorMessage;

	try:

		vConnectionEngine = create_engine(
		URL(
	    	account = vHost,
	    	user = vUser,
	    	password = vPassword,
	    	database = vDatabase,
	    	warehouse = vWarehouse,
	    	role=vRole,
			)
			,encoding='utf-8'
		)

		return vConnectionEngine;

	except:

		vGlobalErrorMessage=traceback.format_exc();
		print("Exception Encountered");
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(0);

def LoadSnowflake(vMarketoObject,vReturnValBulkActivity,vSnowFlakeTable,vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole):

	####################################################################################
	#########Load the Dataframe Created With Delta Record into Snowflake################
	####################################################################################

	global vGlobalErrorMessage;

	####################################################################################
	##########Invokes to Create Connection Engine to the Snowflake Server###############
	####################################################################################

	try:

		vConnectionEngine=CreateDBConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
		print("Snowflake Engine Created");

		vConnection=vConnectionEngine.connect();
		print("Snowflake Connection Created");

		vQuery='DROP TABLE IF EXISTS "'+vSchema+'"."'+vSnowFlakeTable+'"';
		vConnection.execute(vQuery);

		vReturnValBulkActivity.to_sql(vSnowFlakeTable,schema=vSchema,con=vConnectionEngine,if_exists='fail',index=False,chunksize=15000);
		print("Dataframe Loaded in Snowflake Table");

		vConnection.close();
		print("Snowflake Connection Closed");

		vConnectionEngine.dispose();
		print("Snowflake Engine Disconnected");


	####################################################################################
	##########This is to Ignore the Unwanted Index Error as Data is loading#############
	####################################################################################
	except (IndexError):

		print("Dataframe Loaded in Snowflake Table");

		vConnection.close();
		print("Snowflake Connection Closed");

		vConnectionEngine.dispose();
		print("Snowflake Engine Disconnected");


	except:

		vGlobalErrorMessage=traceback.format_exc();
		print("Exception Encountered");
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(0);



def GetMarketoAccessToken(vURL,vClientId,vClientSecret,vVerify):

	####################################################################################
	##########Returns Access Token By Invoking Marketo Authentication API###############
	####################################################################################

	global vGlobalErrorMessage;

	try:

		print("Base URL");
		print(vURL);

		vOAuthURL=vURL+'/identity/oauth/token';
		dataParams={
			    'grant_type': 'client_credentials',
		        'client_id': vClientId,
		        'client_secret': vClientSecret
			}

		requestURL=rqst.get(vOAuthURL,params=dataParams,verify=vVerify);
		outputData=requestURL.json();

		print("Identity Response");
		print(outputData);

		vAccessToken='Bearer '+outputData.get('access_token');
		print("Token ",vAccessToken);

		return vAccessToken;

	except:

		vGlobalErrorMessage=traceback.format_exc();
		print("Exception Encountered");
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(0);


def GetMarketoActivityTypeId(vMarketoObject,vURL,vAccessToken,vStartAt,vEndAt,vVerify):

	####################################################################################
	##########Returns List of Activity Type Pertaining to Marketo Activity##############
	####################################################################################

	global vGlobalErrorMessage;

	global vUser;
	global vPassword;
	global vHost;
	global vDatabase;
	global vSchema;
	global vWarehouse;
	global vRole;

	try:

		vActivityTypeIdURL=vURL+'/rest/v1/'+vMarketoObject+'/types.json';
		vHeadersConfig={
		        'Authorization': vAccessToken
		    }

		ActivityTypeIdURL=rqst.get(vActivityTypeIdURL,headers=vHeadersConfig,verify=vVerify);
		ActivityTypeIdData=ActivityTypeIdURL.json();

		print("Identity Response");
		print(ActivityTypeIdData);

		ActivityTypeIdResult=ActivityTypeIdData.get('result');
		#print(ActivityTypeIdResult);

		####################################################################################
		#####Variable to store the List of Activity TypeID Pertaining to Marketo Activity###
		####################################################################################

		vListActivityTypeId=[];
		vActivityTypeId="";
		vActivityTypeName="";

		####################################################################################
		####Variable to store the Activity TypeID,TypeName Pertaining to Marketo Activity###
		####################################################################################

		vActivityTypeTup=();
		vActivityTypeIDNameFinal=[];

		####################################################################################
		##Variable to store the Activity Type wise Metadata Pertaining to Marketo Activity##
		####################################################################################

		vActivityTypeMetadataTemp=[];
		vActivityTypeMetadata=[];


		for item in ActivityTypeIdResult:

			if (item.get('name') in ("Send Email","Click Email","Email Delivered","Open Email","Click Link","Fill Out Form","Visit Webpage","Unsubscribe Email","Email Bounced Soft","Email Bounced")):

				#print(item);

				vActivityTypeId=item.get('id');
				vActivityTypeName=item.get('name');

				if ('primaryAttribute' in item):

					vActivityTypePrimaryAttrName=item.get('primaryAttribute').get('name');
					vActivityTypePrimaryAttrDataType=item.get('primaryAttribute').get('dataType');

				else:

					vActivityTypePrimaryAttrName="";
					vActivityTypePrimaryAttrDataType="";

				####################################################################################
				##Loop Through Each Attribute Per Aactivity Type ID Pertaining to Marketo Activity##
				####################################################################################

				if 	('attributes' in item):
					ActivityTypeAttributes=item.get('attributes');

					for attr in ActivityTypeAttributes:

						vActivityTypeAddAttrName=attr.get('name');
						vActivityTypeAddAttrDataType=attr.get('dataType');

						vActivityTypeMetadataTemp.append(vActivityTypeId);
						vActivityTypeMetadataTemp.append(vActivityTypeName);
						vActivityTypeMetadataTemp.append(vActivityTypePrimaryAttrName);
						vActivityTypeMetadataTemp.append(vActivityTypePrimaryAttrDataType);
						vActivityTypeMetadataTemp.append(vActivityTypeAddAttrName);
						vActivityTypeMetadataTemp.append(vActivityTypeAddAttrDataType);

						vActivityTypeMetadata.append(vActivityTypeMetadataTemp);
						vActivityTypeMetadataTemp=[];

				else:

					vActivityTypeAddAttrName="";
					vActivityTypeAddAttrDataType="";

					vActivityTypeMetadataTemp.append(vActivityTypeId);
					vActivityTypeMetadataTemp.append(vActivityTypeName);
					vActivityTypeMetadataTemp.append(vActivityTypePrimaryAttrName);
					vActivityTypeMetadataTemp.append(vActivityTypePrimaryAttrDataType);
					vActivityTypeMetadataTemp.append(vActivityTypeAddAttrName);
					vActivityTypeMetadataTemp.append(vActivityTypeAddAttrDataType);

					vActivityTypeMetadata.append(vActivityTypeMetadataTemp);
					vActivityTypeMetadataTemp=[];

				vListActivityTypeId.append(vActivityTypeId);
				vActivityTypeTup=(vActivityTypeId,vActivityTypeName);
				vActivityTypeIDNameFinal.append(vActivityTypeTup);

				vActivityTypeTup=();

		print("vActivityTypeIDNameFinal");
		print(vActivityTypeIDNameFinal);

		print("vActivityTypeMetadata");
		print(vActivityTypeMetadata);

		####################################################################################
		#######Creating a Dataframe to Load Actitvity Type Metadata in Snowflake############
		####################################################################################

		vActivityTypeMetadataDF=pd.DataFrame(data=vActivityTypeMetadata,columns=['ActivityTypeID','ActivityTypeName','ActivityPrimaryAttributeName','ActivityPrimaryAttributeDataType','ActivityAddAttributeName','ActivityAddAttributeDataType'],dtype='string');
		vReturnValBulkActivity=vActivityTypeMetadataDF;
		vSnowFlakeTable='Marketo_'+vMarketoObject+'_Metadata';

		vReturnValBulkActivity['StartAt']=vStartAt;
		vReturnValBulkActivity['EndAt']=vEndAt;

		print("Marketo Object "+vMarketoObject+" Metadata Table Loading Start");
		LoadSnowflake(vMarketoObject,vReturnValBulkActivity,vSnowFlakeTable,vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
		print("Marketo Object "+vMarketoObject+" Metadata Table Loading Complete");

		####################################################################################
		##############Creating a Dataframe to join with Extracted Output####################
		####################################################################################

		vGlobalctivityTypeIdDF=pd.DataFrame(data=vActivityTypeIDNameFinal,columns=['LookupActivityTypeID','LookupActivityTypeName'],dtype='str');

		#print("Generated Activity Type ID Dataframe");
		#print(vGlobalctivityTypeIdDF);

		vListActivityTypeIdNameTup=(vListActivityTypeId,vGlobalctivityTypeIdDF);

		#print("Return Tupple");
		#print(vListActivityTypeIdNameTup);

		return vListActivityTypeIdNameTup;

	except:

		vGlobalErrorMessage=traceback.format_exc();
		print("Exception Encountered");
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(0);


def GetMarketoObjectAPIFields(vMarketoObject,vURL,vAccessToken,vBatchSize,vStartAt,vEndAt,vVerify):

	####################################################################################
	######Returns list of API fields on lead objects in the target instance Starts######
	####################################################################################

	global vGlobalErrorMessage;

	global vUser;
	global vPassword;
	global vHost;
	global vDatabase;
	global vSchema;
	global vWarehouse;
	global vRole;

	try:

		vHeadersConfig={
		        'Authorization': vAccessToken
		    }

		vDataConfig={
		        'batchSize' : vBatchSize
		}

		vSchemaURL=vURL+'/rest/v1/'+vMarketoObject+'/describe2.json';
		requestURL=rqst.get(vSchemaURL,headers=vHeadersConfig,params=vDataConfig,verify=vVerify);
		outputData=requestURL.json();

		print(outputData);

		vResult=outputData.get('result')[0].get('fields');
		vExportFieldList=[];

		vExportFieldMetadataRow=[];
		vExportFieldMetadata=[];

		for apiName in vResult:

			vName='';
			vDisplayName='';
			vDataType='';
			vLength='';

			if 'name' in apiName:
				vName=apiName.get('name');
				vExportFieldList.append(vName);
			else:
				vName='';

			if 'displayName' in apiName:
				vDisplayName=apiName.get('displayName');
			else:
				vDisplayName=''

			if 'dataType' in apiName:
				vDataType=apiName.get('dataType');
			else:
				vDataType='';

			if 'length' in apiName:
				vLength=apiName.get('length');
			else:
				vLength='';

			####################################################################################
			########Create 2 List of List To Store the Metadata Of the Marketo Object###########
			####################################################################################

			vExportFieldMetadataRow.append(vName);
			vExportFieldMetadataRow.append(vDisplayName);
			vExportFieldMetadataRow.append(vDataType);
			vExportFieldMetadataRow.append(vLength);

			vExportFieldMetadata.append(vExportFieldMetadataRow);
			vExportFieldMetadataRow=[];


		vReturnValBulkActivity=pd.DataFrame(vExportFieldMetadata,columns=['name','displayName','dataType','length'],dtype="str");
		vSnowFlakeTable='Marketo_'+vMarketoObject+'_Metadata';

		vReturnValBulkActivity['StartAt']=vStartAt;
		vReturnValBulkActivity['EndAt']=vEndAt;

		print("Marketo Object "+vMarketoObject+" Metadata Table Loading Start");
		LoadSnowflake(vMarketoObject,vReturnValBulkActivity,vSnowFlakeTable,vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
		print("Marketo Object "+vMarketoObject+" Metadata Table Loading Complete");

		return vExportFieldList;

	except:

		vGlobalErrorMessage=traceback.format_exc();
		print("Exception Encountered");
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(0);

####################################################################################
######Returns list of API fields on lead objects in the target instance Ends########
####################################################################################


def GetMarketoObjectExportDeltaBulkData(vMarketoObject,vURL,vClientId,vClientSecret,vSleep,vMaxAcceptedDuration,vBatchSize,vStartAt,vEndAt,vFilterField,vVerify):

	##############################################################################
	######Bulk Export Lead Data Using Specified Filter on Date for Delta Starts###
	##############################################################################

	global vGlobalErrorMessage;

	try:

		##############################################################################
		##########Invoke GetMarketoAccessToken to retrieve the Bearer Token###########
		##############################################################################
		vAccessToken=GetMarketoAccessToken(vURL,vClientId,vClientSecret,vVerify);

		##############################################################################
		########Invoke GetMarketoObjectAPIFields to retrieve API Field List###########
		##############################################################################

		vExtractJobURL=vURL+'/bulk/v1/'+vMarketoObject+'/export/create.json';

		vHeadersConfig={
	         	'Authorization': vAccessToken,
	         	'Content-Type': 'application/json'
	     	}

		if (vMarketoObject == "leads"):

			print("Get List Of Lead Data Type Operation Start");
			vExportFieldList=GetMarketoObjectAPIFields(vMarketoObject,vURL,vAccessToken,vBatchSize,vStartAt,vEndAt,vVerify);
			print("Get List Of Lead Data Type Operation End");

			vDataConfig={
		    	'fields': vExportFieldList,
		    	'format': 'CSV',
		    	'filter': {
		       	vFilterField: {
		          	'startAt': vStartAt,
		          	'endAt': vEndAt
		       	}
		    	}
		 	}


		elif (vMarketoObject == "activities"):

			print("Get List Of Activity Type Operation Start");
			vListActivityTypeIdNameTup=GetMarketoActivityTypeId(vMarketoObject,vURL,vAccessToken,vStartAt,vEndAt,vVerify);
			vListActivityTypeId=vListActivityTypeIdNameTup[0];
			print("Get List Of Activity Type Operation End");

			vDataConfig={
		    	'format': 'CSV',
		    	'filter': {
		       	vFilterField: {
		          	'startAt': vStartAt,
		          	'endAt': vEndAt
		       	},
				'activityTypeIds':vListActivityTypeId
		    	}
		 	}


		vDataConfigJson=json.dumps(vDataConfig);
		print("Body Parameter For Created Bulk Job Post Request");
		print(vDataConfigJson);


		requestDataURL=rqst.post(vExtractJobURL,headers=vHeadersConfig,data=vDataConfigJson,verify=vVerify);
		print('Response '+str(requestDataURL));
		requestData=requestDataURL.json();
		print(requestData);


		vGenRequestID=requestData.get('requestId');
		vGenExportID=requestData.get('result')[0].get('exportId');
		vGenExportJobStatus=requestData.get('result')[0].get('status');

		if (vGenExportJobStatus != "Created"):

			vGlobalErrorMessage="Bulk Export Job Creation Failed";
			print(vGlobalErrorMessage);
			context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
			return -1;

		else:
			print("Export Job Created Successfully");

			#######################################################################
			###############Bulk Export Job Invoke In Background Starts ############
			#######################################################################

			vEnqueueJobURL=vURL+'/bulk/v1/'+vMarketoObject+'/export/'+vGenExportID+'/enqueue.json';
			vHeadersConfig={
        	'Authorization': vAccessToken,
        	'Content-Type': 'application/json'
    	}

		requestEnqueueJobURL=rqst.post(vEnqueueJobURL,headers=vHeadersConfig,verify=vVerify);
		print('Response '+str(requestEnqueueJobURL));
		requestEnqueueJob=requestEnqueueJobURL.json();
		print(requestEnqueueJob);

		vGenEnqueueJobStatus=requestEnqueueJob.get('result')[0].get('status');

		#######################################################################
		###############Bulk Export Job Invoke In Background Ends## ############
		#######################################################################

		if (vGenEnqueueJobStatus != "Queued"):

			vGlobalErrorMessage="Bulk Export Job Enqueue Process Failed";
			print(vGlobalErrorMessage);
			context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
			return -1;

		else:

			#######################################################################
			#########Bulk Export Job Poll to Check the Status Job Starts###########
			#######################################################################
			print("Enqueue Job Initiated Successfully");


			vCounter=0;
			vCummDuration=0;

			vGenPollJobStatus="";

			#######################################################################
			#####While Processing Of Bulk Request Status Will be Polled############
			#######################################################################

			while (1):

				vPollJobURL=vURL+'/bulk/v1/'+vMarketoObject+'/export/'+vGenExportID+'/status.json';
				vHeadersConfig={
		        	'Authorization': vAccessToken,
		        	'Content-Type': 'application/json'
		    	}

				requestPollJobURL=rqst.get(vPollJobURL,headers=vHeadersConfig,verify=vVerify);
				print('Response '+str(requestPollJobURL));
				requestPollJob=requestPollJobURL.json();
				print(requestPollJob);

				vGenPollJobStatus=requestPollJob.get('result')[0].get('status');

				if (vGenPollJobStatus == "Completed"):

					print("Export Completed For Request - "+str(vGenExportID)+" . File Ready For Download");
					print("Export Duration ",vCummDuration);

					vGenFileSize=requestPollJob.get('result')[0].get('fileSize');
					vGenFileRecordCount=requestPollJob.get('result')[0].get('numberOfRecords');
					vGenFileHash=requestPollJob.get('result')[0].get('fileChecksum');

					break;

				elif (vGenPollJobStatus == "Cancelled" or vGenPollJobStatus == "Failed"):

					vGlobalErrorMessage="Export Operation For "+str(vGenExportID)+" Is "+str(vGenPollJobStatus);
					print(vGlobalErrorMessage);
					print("Export Operation Duration Before "+vGenPollJobStatus+" Is "+str(vCummDuration));
					context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
					break;

				elif ((vGenPollJobStatus == "Queued" or vGenPollJobStatus == "Processing" ) and vCummDuration > vMaxAcceptedDuration):

					vGlobalErrorMessage="Export Operation For "+str(vGenExportID)+" Is "+str(vGenPollJobStatus)+" For "+str(vCummDuration)+" Seconds is More than Acceptable Limit Of "+str(vMaxAcceptedDuration)+" Process Will Fail";
					print(vGlobalErrorMessage);
					context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
					break;

				else:
					print("Export Operation In Progress. Iteration No ",vCounter+1);


					time.sleep(vSleep);
					vCounter+=1;
					vCummDuration=vCounter*vSleep;

			#######################################################################
			#########Bulk Export Job Poll to Check the Status Job Ends#############
			#######################################################################

			if (vGenPollJobStatus != "Completed"):

				vGlobalErrorMessage="Bulk Export Status Polling Job Failed";
				print(vGlobalErrorMessage);
				return -1;

			else:
				print("Status Poll Job Completed Successfully");
				print("Exported File Size ",vGenFileSize);
				print("Exported File Record Count ",vGenFileRecordCount);

 				#######################################################################
 				###########Get The Data From Endpoint For the Current Job##############
				#######################################################################

				vExportDataJobURL=vURL+'/bulk/v1/'+vMarketoObject+'/export/'+vGenExportID+'/file.json';

				vHeadersConfig={
 		    		'Authorization': vAccessToken,
 		    		'Content-Type': 'application/json'
 				}

				requestDataJobURL=rqst.get(vExportDataJobURL,headers=vHeadersConfig,verify=vVerify);
				print('Response '+str(requestDataJobURL));
				requestDataJob=requestDataJobURL.text; #Flat Unicode String Output
				#print(requestDataJob);

				#######################################################################
				########Export The Data From Endpoint To Pandas Dataframe##############
				#######################################################################

				outputFileContent=StringIO(requestDataJob);
				outputDFContent=pd.read_csv(outputFileContent,sep=",",engine="python",na_filter=False,na_values=None,keep_default_na=False,dtype="str");

				outputDFContent['StartAt']=vStartAt;
				outputDFContent['EndAt']=vEndAt;

				print("Raw Count of Extracted Object ",len(outputDFContent.index));

				###################################################################################################################################
				##########Creating a Tuple vListActivityTypeIdNameTup with df vGlobalctivityTypeIdDF to join with Extracted Output#################
				###################################################################################################################################

				if (vMarketoObject == "activities"):

					vGlobalctivityTypeIdDF=vListActivityTypeIdNameTup[1];

					print("Lookup DataFrame");
					print(vGlobalctivityTypeIdDF);

					vMergedOutputDFContent=pd.merge(outputDFContent,vGlobalctivityTypeIdDF,how='left',left_on='activityTypeId',right_on='LookupActivityTypeID');
					outputDFContent=vMergedOutputDFContent;
					outputDFContent=outputDFContent.drop(['LookupActivityTypeID'],axis=1);

					print("Record Count In DataFrame Post Merge",len(outputDFContent.index));

				#######################################################################
				########Write Dataframe Content into CSV File For Testing##############
				#######################################################################

				#outputPath='/Users/rajdeep.chakraborty/Desktop/Python/Marketo/POC_Delta_Lead.csv';
				#outputDFContent.to_csv(outputPath,index=True,header=True,mode='w',encoding=None);

				if (vGenFileRecordCount == len(outputDFContent.index)):
					print("Extracted Record Count Matches With Marketo Extract Statistics");
					return outputDFContent;

				else:
					vGlobalErrorMessage="API Extracted Record Count MisMatches With Created DataFrame Marketo Extract Statistics";
					print(vGlobalErrorMessage);
					context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
					return -1;


	except:

		vGlobalErrorMessage=traceback.format_exc();
		print("Exception Encountered");
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(0);

		##############################################################################
		######Bulk Export Lead Data Using Specified Filter on Date for Delta End######
		##############################################################################

if __name__=='__main__':

	###################################################################################
	###############Variables Will be Picked Up From Matillion Jobs#####################
	###################################################################################

	vURL='${EnvVarMarketoBaseURL}';
	vSleep='${EnvVarMarketoSleepInterval}';
	vMaxAcceptedDuration='${EnvVarMarketoMaxAcceptedDuration}';
	vBatchSize='${EnvVarMarketoBatchSize}';
	vClientId='${EnvVarMarketoClientId}';
	vClientSecret='${EnvVarMarketoClientSecret}';
	vVerify=True;
    
    
	vStartAt='${JobVarStartDateTime}';
	vEndAt='${JobVarEndDateTime}';
    
	vMarketoObject='${JobVarMarketoObject}';
	vFilterField='${JobVarMarketoHighWaterMark}';


	################# Parameterized #############################


	###################################################################################
	###########Snowflake Variables Will be Picked Up From Matillion Jobs###############
	###################################################################################

	global vUser;
	global vPassword;
	global vHost;
	global vDatabase;
	global vSchema;
	global vWarehouse;
	global vRole;

	################# Parameterized #############################

	vUser='${EnvVarMarketoUser}';
	vPassword='${EnvVarMarketoPassword}';
	vHost='${EnvVarMarketoHost}';
	vDatabase='${EnvVarMarketoDatabase}';
	vSchema='${EnvVarMarketoSchema}';
	vWarehouse='${EnvVarMarketoWarehouse}';
	vRole='${EnvVarMarketoRole}';

	################# Parameterized #############################

	###################################################################################
	###############Variables Will be Picked Up From Matillion Jobs#####################
	###################################################################################

	global vGlobalErrorMessage;

	vReturnValBulkActivity=GetMarketoObjectExportDeltaBulkData(vMarketoObject,vURL,vClientId,vClientSecret,vSleep,vMaxAcceptedDuration,vBatchSize,vStartAt,vEndAt,vFilterField,vVerify);

	if isinstance(vReturnValBulkActivity,pd.DataFrame):

		print("Dataframe Creation Successfull With Extracted Data For Marketo Object - "+vMarketoObject);
		vSnowFlakeTable='Marketo_'+vMarketoObject+'_TEMP';
		LoadSnowflake(vMarketoObject,vReturnValBulkActivity,vSnowFlakeTable,vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);

	else:

		vGlobalErrorMessage="Dataframe Creation Failed With Extracted Data For Marketo Object - "+vMarketoObject;
		print(vGlobalErrorMessage);
		context.updateVariable('JobVarErrorBulkExportOutputMessage',vGlobalErrorMessage);
		exit(1);
