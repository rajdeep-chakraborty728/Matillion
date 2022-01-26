import string
import sys
import traceback
import boto3
import json
import tableauserverclient as TSC
import time
import datetime
import pytz

# -- Function To Convert Timezone -- #
def convertTimezone(FromTimeZone,ToTimeZone,FromTimeStampStr):

  global vGlobalErrorMessage;

  try:

    if (FromTimeStampStr is None):
      return '';

    else:
      vFromTimeStampStrArr=FromTimeStampStr.split('+');
      vFromTimeStampStr=vFromTimeStampStrArr[0];
      vFromConvertTimeStamp=datetime.datetime.strptime(vFromTimeStampStr,'%Y-%m-%d %H:%M:%S');
      vFromTimeZone=pytz.timezone(FromTimeZone);
      vFromTimeStamp=vFromTimeZone.localize(vFromConvertTimeStamp);
      vToTimeZone=pytz.timezone(ToTimeZone);
      vToTimeStamp=vFromTimeStamp.astimezone(vToTimeZone);

    return str(vToTimeStamp).split(' ')[0]+' '+str(vToTimeStamp).split(' ')[1].split('-')[0];

  except:

    vGlobalErrorMessage=traceback.format_exc();
    print("Exception Encountered");
    print(vGlobalErrorMessage);
    context.updateVariable('JobVarDataSourceRefreshErrorMessage',vGlobalErrorMessage);
    exit(0);

# -- Function To Retrieve Tableau Parameters from AWS Secrets -- #
def getSecrets(inpParamSecretName):

  global vGlobalErrorMessage;

  try:

    vSecretName = inpParamSecretName;
    vRegionName = JobVarAWSServiceRegion;
    vServiceName= JobVarAWSServiceName;
    vServiceTyp = JobVarAWSServiceTyp;
    vServiceVersion = JobVarAWSServiceVersion;


    # -- Create a Secrets Manager client -- #
    session = boto3.session.Session();

    client = session.client(
      service_name=vServiceTyp,
      region_name=vRegionName
    );


    #-- AWS Secret Version -1 means Latest Version of the Secret -- #

    if JobVarAWSServiceVersion == '-1':
      vGetSecretReponse=client.get_secret_value(SecretId=vServiceName);

    else:
      vGetSecretReponse=client.get_secret_value(SecretId=vServiceName,VersionId=vServiceVersion);

    #-- Get the Secret Manager Secret Name From AWS Secret Manager --#

    vGetSecretString=vGetSecretReponse.get('SecretString');
    vGetSecretStringJson=json.loads(vGetSecretString);

    #-- Get the Specific Secret Key From the Secret --#

    return vGetSecretStringJson.get(vSecretName);

  except:

    vGlobalErrorMessage=traceback.format_exc();
    print("Exception Encountered");
    print(vGlobalErrorMessage);
    context.updateVariable('JobVarDataSourceRefreshErrorMessage',vGlobalErrorMessage);
    exit(0);

# -- Function To Retrieve Tableau Parameters from Config -- #
def getParameters():

  global vtabUsername;
  global vtabPassword;
  global vtabServerContentURL;
  global vtabServerURL;
  global vtabUseServerVersion;

  global vGlobalErrorMessage;

  try:

    vGetGridVar=context.getGridVariable('GridVarAWSConfig');

    for i in vGetGridVar:

      #-- getFromSecretsmanager will be defined in Config sheet wherever AWS Secret Manager is used to store credentials --#

      if (i[2] == 'getFromSecretsmanager'):

        if (i[0] == 'USERNAME'):
          vtabUsername=getSecrets(i[1]);

        elif (i[0] == 'PASSWORD'):
          vtabPassword=getSecrets(i[1]);

      else:

        if (i[0] == 'CONTENT_URL'):
          vtabServerContentURL=i[2];

        elif (i[0] == 'SERVER_URL'):
          vtabServerURL=i[2];

        elif (i[0] == 'USE_SERVER_VERSION'):
          vtabUseServerVersion=i[2];

  except:

    vGlobalErrorMessage=traceback.format_exc();
    print("Exception Encountered");
    print(vGlobalErrorMessage);
    context.updateVariable('JobVarDataSourceRefreshErrorMessage',vGlobalErrorMessage);
    exit(0);

# -- Function To Check Tableau Refresh Status -- #
def checkStatusDataSources():

  global vtabUsername;
  global vtabPassword;
  global vtabServerContentURL;
  global vtabServerURL;
  global vtabUseServerVersion;
  global vGlobalErrorMessage;
  global vGlobalMessage;

  vLoopMessage='';
  vJobMessage='';
  vTabSession=-1;
  vJobId='';
  vJobRunningFlag=-1;

  try:

    vJobVarListDataSourceID=JobVarListDataSourceID;
    vJobVarListJobId=JobVarListJobID;
    vJobVarListJobDetails=JobVarListJobDetails;


    if (vJobVarListJobId is None):

      vGlobalMessage='No Tableau data source refresh jobs are in progress currently';
      context.updateVariable('JobVarDataSourceRefreshSuccessMessage', vGlobalMessage);
      vGlobalErrorMessage='';

    else:

      #-- Split Job, Data SOurce, Config and Populate Dictionary for faster retrieval--#

      vJobDetails=vJobVarListJobDetails.split(';');
      vDictDataSourceID={};
      vDictDataSourceName={};
      vDictDataSourceConfig={};

      for vLoopJobDetails in vJobDetails:

        vJobSplit=vLoopJobDetails.split(':');
        vDictDataSourceID[vJobSplit[0]]=vJobSplit[1];
        vDictDataSourceName[vJobSplit[0]]=vJobSplit[2];
        vDictDataSourceConfig[vJobSplit[0]]=vJobSplit[3];

      #-- Create a Tablaeu API Connection Using The Parameters retrieved from Secrets --#

      tableau_auth = TSC.TableauAuth(vtabUsername, vtabPassword, vtabServerContentURL);
      server = TSC.Server(vtabServerURL, use_server_version=vtabUseServerVersion);

      with server.auth.sign_in(tableau_auth):

        vTabSession=1;
        listJobId=vJobVarListJobId.split(';');
        vGridVariable=[];

        vDottedLine='*******************************************************';
        vGlobalMessageFailure=vDottedLine+"\n"+'List of Failure Data Sources are'+"\n"+vDottedLine+"\n";
        vGlobalMessageCancelled=vDottedLine+"\n"+'List of Cancelled Data Sources are'+"\n"+vDottedLine+"\n";
        vGlobalMessageCompleted=vDottedLine+"\n"+'List of Completed Data Sources are'+"\n"+vDottedLine+"\n";
        vGlobalMessageInProgress=vDottedLine+"\n"+'List of In Progress Data Sources are'+"\n"+vDottedLine+"\n";

        for vLoopJobId in listJobId:

          vGridRow=[];

          vLoopDataSourceID=vDictDataSourceID.get(vLoopJobId);
          vLoopDataSourceName=vDictDataSourceName.get(vLoopJobId);
          vLoopDataSourceConfig=vDictDataSourceConfig.get(vLoopJobId);

          vLoopJobMessage = server.jobs.get_by_id(vLoopJobId);

          if (vLoopJobMessage.started_at is None):
            vLoopStarted_at='1900-01-02 00:00:00+00:00';
          else:
            vLoopStarted_at=str(vLoopJobMessage.started_at);

          if (vLoopJobMessage.completed_at is None):
            vLoopCompleted_at='1900-01-02 00:00:00+00:00';
          else:
            vLoopCompleted_at=str(vLoopJobMessage.completed_at);

          if (vLoopJobMessage.progress is None):
            vLoopProgress='0%';
          else:
            vLoopProgress=str(vLoopJobMessage.progress)+'%';

          if (str(vLoopJobMessage.finish_code) == "0"):
            vLoopStatus='Completed';
          elif (str(vLoopJobMessage.finish_code) == "1"):
            vLoopStatus='Failed';
          elif (str(vLoopJobMessage.finish_code) == "2"):
            vLoopStatus='Cancelled';
          else:
            vLoopStatus='In Progress';

          vLoopMessageConvCreated_at=convertTimezone('UTC','America/Los_Angeles',str(vLoopJobMessage.created_at));
          vLoopMessageConvStarted_at=convertTimezone('UTC','America/Los_Angeles',vLoopStarted_at);
          vLoopMessageConvCompleted_at=convertTimezone('UTC','America/Los_Angeles',vLoopCompleted_at);

          if (vLoopStatus == 'Failed'):
            vGlobalMessageFailure=vGlobalMessageFailure+'Refresh Status For DataSource - '+vLoopDataSourceName+"\n"+'Extract refresh Job ID - ' + vLoopJobId + "\n" +'Extract refresh created at (PST) - '+ vLoopMessageConvCreated_at+ "\n" +'Extract refresh started at (PST) - ' + vLoopMessageConvStarted_at + "\n" + 'Extract refresh completed at (PST) - ' + vLoopMessageConvCompleted_at + "\n"+ 'Extract refresh progress is - ' + str(vLoopProgress) +"\n"+ 'Extract refresh status is - ' + str(vLoopStatus)+ "\n"+"\n";

          elif (vLoopStatus == 'Cancelled'):
            vGlobalMessageCancelled=vGlobalMessageCancelled+'Refresh Status For DataSource - '+vLoopDataSourceName+"\n"+'Extract refresh Job ID - ' + vLoopJobId + "\n" +'Extract refresh created at (PST) - '+ vLoopMessageConvCreated_at+ "\n" +'Extract refresh started at (PST) - ' + vLoopMessageConvStarted_at + "\n" + 'Extract refresh completed at (PST) - ' + vLoopMessageConvCompleted_at + "\n"+ 'Extract refresh progress is - ' + str(vLoopProgress) +"\n"+ 'Extract refresh status is - ' + str(vLoopStatus)+ "\n"+"\n";

          elif (vLoopStatus == 'Completed'):
            vGlobalMessageCompleted=vGlobalMessageCompleted+'Refresh Status For DataSource - '+vLoopDataSourceName+"\n"+'Extract refresh Job ID - ' + vLoopJobId + "\n" +'Extract refresh created at (PST) - '+ vLoopMessageConvCreated_at+ "\n" +'Extract refresh started at (PST) - ' + vLoopMessageConvStarted_at + "\n" + 'Extract refresh completed at (PST) - ' + vLoopMessageConvCompleted_at + "\n"+ 'Extract refresh progress is - ' + str(vLoopProgress) +"\n"+ 'Extract refresh status is - ' + str(vLoopStatus)+ "\n"+"\n";

          elif (vLoopStatus == 'In Progress'):
            vGlobalMessageInProgress=vGlobalMessageInProgress+'Refresh Status For DataSource - '+vLoopDataSourceName+"\n"+'Extract refresh Job ID - ' + vLoopJobId + "\n" +'Extract refresh created at (PST) - '+ vLoopMessageConvCreated_at+ "\n" +'Extract refresh started at (PST) - ' + vLoopMessageConvStarted_at + "\n" + 'Extract refresh completed at (PST) - ' + vLoopMessageConvCompleted_at + "\n"+ 'Extract refresh progress is - ' + str(vLoopProgress) +"\n"+ 'Extract refresh status is - ' + str(vLoopStatus)+ "\n"+"\n";

          vGridRow.append(vLoopDataSourceConfig);
          vGridRow.append(vLoopDataSourceName);
          vGridRow.append(vLoopDataSourceID);
          vGridRow.append(vLoopJobId);
          vGridRow.append(str(vLoopJobMessage.created_at));
          vGridRow.append(vLoopStarted_at);
          vGridRow.append(vLoopCompleted_at);
          vGridRow.append(vLoopProgress);
          vGridRow.append(vLoopStatus);

          vGridVariable.append(vGridRow);

          time.sleep(2);

          vLoopMessage='';
          vLoopStarted_at='';
          vLoopCompleted_at='';
          vLoopProgress='';
          vStatus='';

          vLoopMessageConvCreated_at='';
          vLoopMessageConvStarted_at='';
          vLoopMessageConvCompleted_at='';

        vGlobalMessage=vGlobalMessageFailure+vGlobalMessageCancelled+vGlobalMessageCompleted+vGlobalMessageInProgress;

        context.updateVariable('JobVarDataSourceRefreshSuccessMessage', vGlobalMessage);
        context.updateGridVariable('GridVarLog',vGridVariable);

        print(context.getGridVariable('GridVarLog'));

      server.auth.sign_out();

  except:

    vGlobalErrorMessage=traceback.format_exc();
    print("Exception Encountered");
    print(vGlobalErrorMessage);
    context.updateVariable('JobVarDataSourceRefreshErrorMessage',vGlobalErrorMessage);

    if (vTabSession == 1):
      server.auth.sign_out();

    exit(0);

if __name__=='__main__':

  #-- Tableau Credential & Success/Error Variables are declared as Global To Access From Any Method --#

  global vtabUsername;
  global vtabPassword;
  global vtabServerContentURL;
  global vtabServerURL;
  global vtabUseServerVersion;
  global vGlobalErrorMessage;
  global vGlobalMessage;

  vGlobalMessage='';

  getParameters();
  checkStatusDataSources();
