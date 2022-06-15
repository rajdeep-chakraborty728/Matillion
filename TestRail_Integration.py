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

        vDataURL="https://cohesity.testrail.com/index.php?/api/v2/get_suites/4";

        requestDataURL=rqst.get(vDataURL,headers=vDataHeadersConfig,verify=False);

        #print("List Of Suites");

        outputData=requestDataURL.json();
        #print(len(outputData));
        vCount=0;

        vColumns = ['id', 'name', 'project_id','is_master','is_baseline','is_completed','completed_on','url'];
        vLoopSuitesList=[];

        for vLoopSuite in outputData:

            #print(vLoopSuite);
            vLoopSuitesList.append([
                                vLoopSuite.get('id'),
                                vLoopSuite.get('name'),
                                vLoopSuite.get('project_id'),
                                vLoopSuite.get('is_master'),
                                vLoopSuite.get('is_baseline'),
                                vLoopSuite.get('is_completed'),
                                vLoopSuite.get('completed_on'),
                                vLoopSuite.get('url'),
                                ]);
            vCount+=1;
            if (vCount > 5000):
                break;

        dfSuites = pd.DataFrame(vLoopSuitesList,columns=vColumns);
        #print(dfSuites);

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

        vDataURL="https://cohesity.testrail.com/index.php?/api/v2/get_cases/4";

        vCaseCount=0;
        vListCases=[];
        vColumns = ['id','title','section_id','type_id','priority_id','created_on','updated_on','suite_id','milestone_id','is_frontend_tc'
        ,'custom_automation_status','custom_added_in_release','custom_is_regression','custom_squad_name','custom_automation_owner'
        ,'custom_tc_added_to_train','custom_automation_target_date','custom_automated_test_case_name','custom_customer_found'
        ,'custom_is_dmaas','display_order','estimate','estimate_forecast','is_deleted','custom_customer_found_defect_id','custom_automation_type'
        ];

        for index,record in vInpDFSuiteList.iterrows():
            print(record['id']);
            vCount=0;

            while (True):

                vDataURL="https://cohesity.testrail.com/index.php?/api/v2/get_cases/4";
                vDataParams={
                    'suite_id' : record['id'],
                    'limit' : vLimit,
                    'offset' : vCount
                    };

                requestDataURL=rqst.get(vDataURL,headers=vDataHeadersConfig,params=vDataParams,verify=False);
                outputData=requestDataURL.json();
                vCaseCount=vCaseCount+len(outputData.get('cases'));

                #print(outputData.get('cases'));

                for idx,val in enumerate(outputData.get('cases')):
                    #print(val);
                    vListCases.append([
                                        val.get('id'),
                                        val.get('title'),
                                        val.get('section_id'),
                                        val.get('type_id'),
                                        val.get('priority_id'),
                                        val.get('created_on'),
                                        val.get('updated_on'),
                                        val.get('suite_id'),
                                        val.get('milestone_id'),
                                        val.get('is_frontend_tc'),
                                        val.get('custom_automation_status'),
                                        val.get('custom_added_in_release'),
                                        val.get('custom_is_regression'),
                                        val.get('custom_squad_name'),
                                        val.get('custom_automation_owner'),
                                        val.get('custom_tc_added_to_train'),
                                        val.get('custom_automation_target_date'),
                                        val.get('custom_automated_test_case_name'),
                                        val.get('custom_customer_found'),
                                        val.get('custom_is_dmaas'),
                                        val.get('display_order'),
                                        val.get('estimate'),
                                        val.get('estimate_forecast'),
                                        val.get('is_deleted'),
                                        val.get('custom_customer_found_defect_id'),
                                        val.get('custom_automation_type')
                                        ]);

                    #vListCases.append(list(val.values()));

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

    vLimit=250;

	################# Hardcoded #################################
    print("--Get Suites API Called--");
    vDFSuiteList=getSuites();
    print("--Get Suites API Finished--");

    print("--Suites Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,'TESTRAIL_SUITES',vDFSuiteList);
    print("--Suites Load to Snowflake Finished--");

    print("--Get Cases API Called--");
    vDFCaseList=getCases(vDFSuiteList);
    print("--Get Cases API Finished--");

    print("--Cases Load to Snowflake Started--");
    LoadSnowflake(vUser,vPassword,vHost,vWarehouse,vDatabase,vSchema,vRole,'TESTRAIL_CASES',vDFCaseList);
    print("--Cases Load to Snowflake Finished--");
