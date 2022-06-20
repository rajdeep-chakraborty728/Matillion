import requests as rqst
import json
import base64
import datetime
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import snowflake.connector
import snowflake.connector.errors
import sys
import traceback
from TestRailCredentials import (User,Password,Host,Database,Schema,Warehouse,Role,TestrailUser,TestrailPassword)
from TestRailConfigs import (SuiteColumns,CaseColumns,MilestoneColumns,ProjectId,CohesityTestRailBaseURL,ConfigNoOfSuites,APILimit,SnowflakeObjectMapping,TestRailEPochDate)

snowflake.connector.paramstyle='qmark';

def CreateSnowflakeConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole):

    global vGlobalErrorMessage;

    try:

        ctx = snowflake.connector.connect(
            user=vUser,
            password=vPassword,
          	role=vRole,
            account=vHost,
            warehouse=vWarehouse,
            database=vDatabase
        );

        return ctx;

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

def CreateSnowflakeAlchemyDBConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole):

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

def getHighWatermark(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,vInputObject,SnowflakeObjectMapping):

    global vGlobalErrorMessage;
    global vMinEpochTime;

    try:

        vSFSchema=SnowflakeObjectMapping.get('HighWatermark')[0];
        vSFObject=SnowflakeObjectMapping.get('HighWatermark')[1];
        vObjectID=SnowflakeObjectMapping.get(vInputObject)[0];


        vConnection=CreateSnowflakeConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
        vCursor=vConnection.cursor();

        vQuery="""SELECT
                    COUNT(1) AS CNT
                FROM """+vSFSchema+"""."""+vSFObject+"""
                WHERE 1=1 AND OBJ_ID=?
                """;

        vCursor.execute(vQuery,[vObjectID]);

        vCountARR=vCursor.fetchone();
        vCount=vCountARR[0];

        vRet=-1;

        if (vCount == 0):
            vRet=vMinEpochTime;

        else:

            vQuery="""SELECT
                        NVL(UPDATED_ON,"""+str(vMinEpochTime)+""") AS UPDATED_ON
                    FROM """+vSFSchema+"""."""+vSFObject+"""
                    WHERE 1=1 AND OBJ_ID=?
                    """;

            vCursor.execute(vQuery,[vObjectID]);

            vUpdatedOnARR=vCursor.fetchone();
            vRet=vUpdatedOnARR[0];

        vConnection.close();
        vCursor.close();

        print("High Watermark For Object "+vInputObject+" Is "+str(vRet));
        return vRet+1;

    except:
        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

def insertHighWatermark(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,vInputObject,SnowflakeObjectMapping):

    global vGlobalErrorMessage;

    try:

        vSFSchema=SnowflakeObjectMapping.get('HighWatermark')[0];
        vSFObject=SnowflakeObjectMapping.get('HighWatermark')[1];

        vObjectID=SnowflakeObjectMapping.get(vInputObject)[0];
        vSFObjectTable=SnowflakeObjectMapping.get(vInputObject)[1];
        vHWMColumn=SnowflakeObjectMapping.get(vInputObject)[2];

        vConnection=CreateSnowflakeConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
        vCursor=vConnection.cursor();

        vQuery="""SELECT MAX("""+vHWMColumn+""") FROM """+vSchema+"""."""+vSFObjectTable;
        vCursor.execute(vQuery);

        vHWMARR=vCursor.fetchone();
        vHWM=vHWMARR[0];


        vQuery="""DELETE FROM """+vSFSchema+"""."""+vSFObject+"""
                WHERE 1=1 AND OBJ_ID = ? """;

        vCursor.execute(vQuery,[vObjectID]);

        vQuery="""INSERT INTO """+vSFSchema+"""."""+vSFObject+"""
                (
                    OBJ_ID,
                    OBJ_NM,
                    TBL_NM,
                    UPDATED_ON
                )
                SELECT
                    ?,
                    ?,
                    ?,
                    ?
                """;

        vCursor.execute(vQuery,[vObjectID,vInputObject,vSFObjectTable,vHWM]);

        vConnection.close();
        vCursor.close();

    except:
        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);

def LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,vSnowFlakeTable,vDataFrame):

    global vGlobalErrorMessage;

    try:

        vConnectionEngine=CreateSnowflakeAlchemyDBConnection(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole);
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

def getCases(vInpDFSuiteList,vHWM):

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
                    'updated_after' : vHWM,
                    'limit' : vLimit,
                    'offset' : vCount
                    };

                requestDataURL=rqst.get(vDataURL,headers=vDataHeadersConfig,params=vDataParams,verify=False);
                outputData=requestDataURL.json();

                if (outputData.get('cases') is None):
                    break;

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

def getMilestones():

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

        vMilestoneCount=0;
        vListMilestones=[];
        vColumns = MilestoneColumns;

        vCount=0;

        while (True):

            vDataURL=CohesityTestRailBaseURL+"get_milestones/"+str(ProjectId);
            vDataParams={
                'limit' : vLimit,
                'offset' : vCount
                };

            requestDataURL=rqst.get(vDataURL,headers=vDataHeadersConfig,params=vDataParams,verify=False);
            outputData=requestDataURL.json();

            if (outputData.get('milestones') is None):
                break;

            vMilestoneCount=vMilestoneCount+len(outputData.get('milestones'));

            for idx,val in enumerate(outputData.get('milestones')):

                vLoopCurrList=[];
                for j in vColumns:
                    vLoopCurrList.append(val.get(j));

                vListMilestones.append(vLoopCurrList);

            if (outputData.get('size') < vLimit):
                break;

            else:
                vCount+=outputData.get('size');

        dfMilestones = pd.DataFrame(vListMilestones,columns=vColumns);
        return dfMilestones;

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        exit(0);


def getSubMilestones(vInpDfMilestone):

    global vLimit;
    global vAuth;

    try:

        vListSubMilestones=[];
        vColumns = MilestoneColumns;

        for j in vInpDfMilestone.get('milestones'):

            if (len(j) > 0):

                for vLoopSubMilestone in j:

                    vLoopCurrList=[];
                    for j in vColumns:
                        vLoopCurrList.append(vLoopSubMilestone.get(j));

                    vListSubMilestones.append(vLoopCurrList);

        dfSubMilestones = pd.DataFrame(vListSubMilestones,columns=vColumns);
        #vInpDfMilestone.drop('milestones',axis=1, inplace = True);

        dfModSubMilestones=pd.concat([vInpDfMilestone, dfSubMilestones], axis=0);
        dfModSubMilestones.drop('milestones',axis=1, inplace = True);

        return dfModSubMilestones;


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
    global vMinEpochTime;

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
    vMinEpochTime=TestRailEPochDate;

	################# Hardcoded #################################

    print("--Get Suites API Called--");
    vDFSuiteList=getSuites();
    print("--Get Suites API Finished--");

    print("--Suites Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,SnowflakeObjectMapping.get('Suites')[1],vDFSuiteList);
    print("--Suites Load to Snowflake Finished--");

    print("--Get High Watermark For Cases Started--");
    vGetHWCases=getHighWatermark(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,'Cases',SnowflakeObjectMapping);
    print("--Get High Watermark For Cases Finished--");

    print("--Get Cases API Called--");
    vDFCaseList=getCases(vDFSuiteList,vGetHWCases);
    print("--Get Cases API Finished--");

    print("--Cases Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,SnowflakeObjectMapping.get('Cases')[1],vDFCaseList);
    print("--Cases Load to Snowflake Finished--");

    print("--Get Milestones API Called--");
    vDFMilestoneList=getMilestones();
    print("--Get Milestones API Finished--");

    print("--Generate Sub Milestones Flatten Called--");
    vDFCombineMilestoneList=getSubMilestones(vDFMilestoneList);
    print("--Generate Sub Milestones Flatten Finished--");

    print("--Milestones Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,SnowflakeObjectMapping.get('Milestones')[1],vDFCombineMilestoneList);
    print("--Milestones Load to Snowflake Finished--");


    print("--Cases Highwatermark Insert Started--");
    insertHighWatermark(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,'Cases',SnowflakeObjectMapping);
    print("--Cases Highwatermark Insert Finished--");
