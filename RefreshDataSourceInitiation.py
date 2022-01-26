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

# -- Function To Refresh Tableau Data Sources -- #
def refreshDataSources():

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

    #-- If Empty Data Source Is Passed From Parent / Invoker Job --#

    if (vJobVarListDataSourceID is None):

      vGlobalErrorMessage='No data sources are passed as parameter for refresh. Process Terminates';
      context.updateVariable('JobVarDataSourceRefreshErrorMessage', vGlobalErrorMessage);
      exit(0);

    else:

      #-- Create a Tablaeu API Connection Using The Parameters retrieved from Secrets --#

      tableau_auth = TSC.TableauAuth(vtabUsername, vtabPassword, vtabServerContentURL);
      server = TSC.Server(vtabServerURL, use_server_version=vtabUseServerVersion);

      with server.auth.sign_in(tableau_auth):

        vTabSession=1;
        listDataSourceId=vJobVarListDataSourceID.split(';');
        vGridVariable=[];

        #-- For Each Data Source ID from Input, Data Source Refresh API's will be triggered --#

        for vDataSourceId in listDataSourceId:

          vDataSource = server.datasources.get_by_id(vDataSourceId);
          vLoopMessage = server.datasources.refresh(vDataSource);
          vJobId=vLoopMessage.id;

          vGlobalMessage=vGlobalMessage+'Refresh Initiated For DataSources - '+vDataSource.name+"\n"+'Extract refresh Job ID - ' + str(vJobId) + "\n" +'Extract refresh created at (PST) - ' + convertTimezone('UTC','America/Los_Angeles',str(vLoopMessage.created_at))+"\n"+"\n";

          vGridRow=[];

          vGridRow.append(SharedJobVarInputDataSourceConfig);
          vGridRow.append(vDataSource.name);
          vGridRow.append(vDataSourceId);
          vGridRow.append(vJobId);
          vGridRow.append(str(vLoopMessage.created_at));
          vGridRow.append('1900-01-02 00:00:00+00:00');
          vGridRow.append('1900-01-02 00:00:00+00:00');
          vGridRow.append('0%');
          vGridRow.append('Created');

          vGridVariable.append(vGridRow);

          time.sleep(5);
          vLoopMessage='';


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
  refreshDataSources();
