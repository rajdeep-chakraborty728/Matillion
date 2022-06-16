import requests as rqst
import json
import base64
import datetime
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import sys
import traceback
from TestRailCredentials import (User,Password,Host,Database,Schema,Warehouse,Role,TestrailUser,TestrailPassword)
from TestRailConfigs import (SuiteColumns,CaseColumns,ProjectId,CohesityTestRailBaseURL,ConfigNoOfSuites,APILimit,SnowflakeSuiteTable,SnowflakeCaseTable)

def CreateDBConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole):

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
		);

        return vConnectionEngine;

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

def LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,vSnowFlakeTable,vDataFrame):

    global vGlobalErrorMessage;

    try:

        vConnectionEngine=CreateDBConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
        vConnection=vConnectionEngine.connect();
        vQuery='DROP TABLE IF EXISTS "'+vSchema+'"."'+vSnowFlakeTable+'"';
        vConnection.execute(vQuery);
        vDataFrame.to_sql(vSnowFlakeTable,schema=vSchema,con=vConnectionEngine,if_exists='replace',index=False,chunksize=15000);
        vConnection.close();
        vConnectionEngine.dispose();

    except (IndexError):

        vConnection.close();
        vConnectionEngine.dispose();

    except:
        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

def getSuites():

    global vTestrailUser;
    global vTestrailPassword;
    global vLimit;
    global vAuth;

    try:

        vDataHeadersConfig={
            'Content-Type': 'application/json',
            'Accept-Charset': 'UTF-8',
            'Authorization': 'Basic '+vAuth
            };

        vDataURL=CohesityTestRailBaseURL+"get_suites/"+str(ProjectId);
        requestDataURL=rqst.get(vDataURL,headers=vDataHeadersConfig,verify=False);
        outputData=requestDataURL.json();
        vCount=0;

        vColumns = SuiteColumns;
        vLoopSuitesList=[];

        for vLoopSuite in outputData:

            vLoopCurrList=[];
            for j in vColumns:
                vLoopCurrList.append(vLoopSuite.get(j));

            vLoopSuitesList.append(vLoopCurrList);

            vCount+=1;
            if (vCount > ConfigNoOfSuites):
                break;

        dfSuites = pd.DataFrame(vLoopSuitesList,columns=vColumns);

        return dfSuites;

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

def getCases(vInpDFSuiteList):

    global vTestrailUser;
    global vTestrailPassword;
    global vLimit;
    global vAuth;

    try:

        vDataHeadersConfig={
            'Content-Type': 'application/json',
            'Accept-Charset': 'UTF-8',
            'Authorization': 'Basic '+vAuth
            };

        vCaseCount=0;
        vListCases=[];
        vColumns = CaseColumns;

        for index,record in vInpDFSuiteList.iterrows():
            print(record['id']);
            vCount=0;

            while (True):

                vDataURL=CohesityTestRailBaseURL+"get_cases/"+str(ProjectId);
                vDataParams={
                    'suite_id' : record['id'],
                    'limit' : vLimit,
                    'offset' : vCount
                    };

                requestDataURL=rqst.get(vDataURL,headers=vDataHeadersConfig,params=vDataParams,verify=False);
                outputData=requestDataURL.json();
                vCaseCount=vCaseCount+len(outputData.get('cases'));

                for idx,val in enumerate(outputData.get('cases')):

                    vLoopCurrList=[];
                    for j in vColumns:
                        vLoopCurrList.append(val.get(j));

                    vListCases.append(vLoopCurrList);

                if (outputData.get('size') < vLimit):
                    break;

                else:
                    vCount+=outputData.get('size');

        dfCases = pd.DataFrame(vListCases,columns=vColumns);
        return dfCases;

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

if __name__=='__main__':

    global vUser;
    global vPassword;
    global vHost;
    global vDatabase;
    global vSchema;
    global vWarehouse;
    global vRole;
    global vGlobalErrorMessage;
    global vTestrailUser;
    global vTestrailPassword;
    global vLimit;
    global vAuth;

    vUser=User;
    vPassword=Password;
    vHost=Host;
    vDatabase=Database;
    vSchema=Schema;
    vWarehouse=Warehouse;
    vRole=Role;

    vTestrailUser = TestrailUser;
    vTestrailPassword = TestrailPassword;

    vAuth = str(
        base64.b64encode(
        bytes('%s:%s' % (vTestrailUser, vTestrailPassword), 'utf-8')
        ),
        'ascii'
        ).strip();

    vLimit=APILimit;

	################# Hardcoded #################################
    print("--Get Suites API Called--");
    vDFSuiteList=getSuites();
    print("--Get Suites API Finished--");

    print("--Suites Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,SnowflakeSuiteTable,vDFSuiteList);
    print("--Suites Load to Snowflake Finished--");

    print("--Get Cases API Called--");
    vDFCaseList=getCases(vDFSuiteList);
    print("--Get Cases API Finished--");

    print("--Cases Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,SnowflakeCaseTable,vDFCaseList);
    print("--Cases Load to Snowflake Finished--");

	
