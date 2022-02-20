import gitlab
import base64
import sqlparse
import snowflake.connector
import snowflake.connector.errors
import sys
import traceback
import datetime

snowflake.connector.paramstyle='qmark';
sys.setrecursionlimit(9500);

def CompareDataVaultGITConfig(vBusFolder,vStageFolder,vScriptName,vSnowflakeCursorDataVault,vSnowflakeCursorLog):

    global vGlobalErrorMessage;
    global vGlobalMessage;

    try:

        vExistCount=-1;
        vSQL='';

        vSQL="""SELECT
                    COUNT(1) AS CNT
                FROM MATILLION_TEMP.GIT_SCRIPT_CONFIG_DV_SNOWFLAKE
                WHERE 1=1
                    AND BUSINESS_AREA = ?
                    AND STAGE = ?
                    AND SCRIPT_NAME = ?
                """;

        vSnowflakeCursorDataVault.execute(vSQL,[vBusFolder,vStageFolder,vScriptName]);

        vExistCountArr=vSnowflakeCursorDataVault.fetchone();
        print(vExistCountArr);

        vExistCount=vExistCountArr[0];

        print('Exist Count '+str(vExistCount));

        if vExistCount == 1:
            return '1';

        else:
            return '0';

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print(vGlobalErrorMessage);
        return vGlobalErrorMessage;


def CreateSnowflakeConnection(vInpDatabase):

    global vUsername;
    global vPassword;
    global vAccount;
    global vWarehouse;
    global vDatabase;
    global vSchema;
    global vLogDatabase;
    global vLogSchema;

    global vGlobalErrorMessage;
    global vGlobalMessage;

    try:

        ctx = snowflake.connector.connect(
            user=vUsername,
            password=vPassword,
            account=vAccount,
            warehouse=vWarehouse,
            database=vInpDatabase
        );

        return ctx;

    except:

        vGlobalErrorMessage=traceback.format_exc();
        print("Exception Encountered");
        print(vGlobalErrorMessage);
        context.updateVariable('JobVarErrorMessage',vGlobalErrorMessage);
        exit(0);

def executeDVScripts():

    global vUsername;
    global vPassword;
    global vAccount;
    global vWarehouse;
    global vDatabase;
    global vSchema;
    global vLogDatabase;
    global vLogSchema;

    global vToken;
    global vURL;
    global vProject;
    global vBranch;
    global vBusAreaFolder;
    global vStageFolder;

    global vGlobalErrorMessage;
    global vGlobalMessage;

    try:

        vGlobalErrorMessage='';
        vGlobalMessage='';
        vSnowflakeDataVaultFlag=-1;
        vID=-1;
        vSQL='';
        vLoopStatus='No Status';
        vSnowflakeConnectionDataVaultFlag=False;
        vSnowflakeCursorLogFlag=False;
        vLoopExceptStatus='No Status';

        ########################################################
        # ---- Connect to GITLAB Account via API Start------- #
        ########################################################

        gl=gitlab.Gitlab(vURL, private_token=vToken, api_version=4, ssl_verify=False);
        print("GITLAB Instantiated");
        gl.auth();

        ########################################################
        # ---- Connect to GITLAB Account via API End------- #
        ########################################################

        print("GITLAB Connected");

        ########################################################
        # ---- Get the GITLAB Project Start------- #
        ########################################################

        vProject = gl.projects.get(vProject);

        ########################################################
        # ---- Get the GITLAB Project End------- #
        ########################################################

        ########################################################
        # ---- List the Functional Area & Stages Start------- #
        # ---- Set The Branch To Master ----- #
        ########################################################

        vConfigSchemaList=vBusAreaFolder.split(';');
        vConfigDVStagesList=vStageFolder.split(';');
        vBranch=vBranch;

        ########################################################
        # ---- List the Functional Area & Stages End------- #
        # ---- Set The Branch To Master ----- #
        ########################################################

        ########################################################
        # ---- Open a Session with Data Vault Database Start ------- #
        ########################################################

        vSnowflakeConnectionDataVault=CreateSnowflakeConnection(vDatabase);
        vSnowflakeConnectionDataVault.autocommit(False);
        vSnowflakeCursorDataVault=vSnowflakeConnectionDataVault.cursor();
        vSnowflakeConnectionDataVaultFlag=True;

        ########################################################
        # ---- Open a Session with Data Vault Database End ------- #
        ########################################################

        ########################################################
        # ---- Open a Session with Log Database Start ------- #
        ########################################################

        vSnowflakeConnectionLog=CreateSnowflakeConnection(vLogDatabase);
        vSnowflakeCursorLog=vSnowflakeConnectionLog.cursor();
        vSnowflakeCursorLogFlag=True;

        ########################################################
        # ---- Open a Session with Log Database End ------- #
        ########################################################

        ########################################################
        # ---- Insert a Entry in Log Master Start ------- #
        # ---- Retrieve Generated ID, Haskey -------#
        ########################################################

        vSQL= """ INSERT INTO """+vLogSchema+""".LOG_DV_EXECUTION_MSTR
        (
            EXEC_HASHKEY,
            PROCESS_NM,
            EXEC_DT_KEY,
            EXEC_STRT_TM,
            EXEC_END_TM,
            EXEC_STATUS,
            LAST_UPDT_TM
        )
        SELECT
            md5('Data Vault Scripts'||';'||CURRENT_TIMESTAMP)   AS EXEC_HASHKEY,
            ?                                                   AS PROCESS_NM,
            TO_NUMBER(TO_CHAR(current_timestamp,'YYYYMMDD'))    AS EXEC_DT_KEY,
            CURRENT_TIMESTAMP                                   AS EXEC_STRT_TM,
            NULL                                                AS EXEC_END_TM,
            ?                                                   AS EXEC_STATUS,
            CURRENT_TIMESTAMP                                   AS EXEC_STRT_TM
        """;

        vSnowflakeCursorLog.execute(vSQL,['Data Vault Scripts','0-Started']);

        vSQL="""SELECT
                    EXEC_HASHKEY,
                    ID
                FROM """+vLogSchema+""".LOG_DV_EXECUTION_MSTR
                WHERE ID = (SELECT MAX(ID) FROM """+vLogSchema+""".LOG_DV_EXECUTION_MSTR)
                                    """;
        vSnowflakeCursorLog.execute(vSQL);

        vMSTRHashKeyArr=vSnowflakeCursorLog.fetchone();
        vMSTRHashKey=vMSTRHashKeyArr[0];
        vID=int(vMSTRHashKeyArr[1]);

        ########################################################
        # ---- Insert a Entry in Log Master End ------- #
        # ---- Retrieve Generated ID, Haskey -------#
        ########################################################

        ########################################################
        # ---- Delete The Current Session Log Database Start ------- #
        ########################################################

        vSQL="""DELETE FROM """+vLogSchema+""".LOG_DV_EXECUTION_CURR""";
        vSnowflakeCursorLog.execute(vSQL);

        ########################################################
        # ---- Delete The Current Session Log Database End ------- #
        ########################################################


        vSnowflakeFlag=1;
        vCount=0;
        vLoopSQLSplitCount=0;

        ########################################################
        # ---- Loop though Functional Area & Branches Start ------- #
        # ---- Functional Area & Branches will loop in sorted Manner to ensure Staging -> HLS -> Bus Vault ---- #
        ########################################################

        #for vLoopConfigSchemaList in vConfigSchemaList:
        for vLoopConfigDVStagesList in vConfigDVStagesList:

            #for vLoopConfigDVStagesList in vConfigDVStagesList:
            for vLoopConfigSchemaList in vConfigSchemaList:

                vNestedLoopPath='Scripts/DataVault/'+vLoopConfigSchemaList+'/'+vLoopConfigDVStagesList;

                ########################################################
                # ---- get The Full Tree Within the GIT Project ------- #
                ########################################################

                vListItems = vProject.repository_tree(path=vNestedLoopPath, ref=vBranch,per_page=100);

                if len(vListItems) > 0:

                    for vItem in vListItems:

                        ########################################################
                        # ---- Ignore the commited version history script under each directory ------- #
                        ########################################################

                        if (CompareDataVaultGITConfig(vLoopConfigSchemaList,vLoopConfigDVStagesList,vItem['name'],vSnowflakeCursorDataVault,vSnowflakeCursorLog) == '1'):

                            print("Counter Is ",str(vCount));

                            print("Inside Items");

                            ########################################################
                            # ---- Get the GITLAB Contents in encoded version of Each File & Decode Start ------- #
                            ########################################################

                            vFileInfo = vProject.repository_blob(vItem['id']);
                            vContent = base64.b64decode(vFileInfo['content']).decode('utf-8');

                            ########################################################
                            # ---- Get the GITLAB Contents in encoded version of Each File & Decode End ------- #
                            ########################################################

                            vCount+=1;

                            print("Before Split");

                            ########################################################
                            # ---- Split the SQL Contents by ; and Append with a newline for better readability Start ------- #
                            ########################################################

                            vListSQLStatements=sqlparse.split(vContent,';');

                            print("After Split");

                            vParsedListSQLStatements='';
                            vLoopScript='';
                            vLoopCurrentTimestampStr='';
                            vLoopSQLSplitCount=0;

                            vLoopCurrentTimestampStr=str(datetime.datetime.today()).split(' ')[0]+' '+str(datetime.datetime.today()).split(' ')[1].split('.')[0];
                            vLoopScript=vNestedLoopPath+'/'+vItem['name'];
                            vLoopStatus='0-Started';

                            print("Starting Pre Log Insert "+str(vLoopSQLSplitCount));

                            ###########################################################
                            # ---- Insert an entry in Log table for Initiation - Start------- #
                            ###########################################################

                            vSQL= ("""
                            INSERT INTO """+vLogSchema+""".LOG_DV_EXECUTION_CURR
                            (SEQ_NO,MSTR_EXEC_HASHKEY,SUBJ_AREA,STAGE,EXEC_SCRIPT,EXEC_SCRIPT_SEQ,EXEC_SCRIPT_CONT,EXEC_TM,EXEC_STATUS)
                            VALUES
                            (
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?
                            )
                            """);

                            vSnowflakeCursorLog.execute(vSQL,[vCount,vMSTRHashKey,vLoopConfigSchemaList,vLoopConfigDVStagesList,vLoopScript,0,vContent,vLoopCurrentTimestampStr,vLoopStatus]);

                            ###########################################################
                            # ---- Insert an entry in Log table for Initiation - End------- #
                            ###########################################################

                            print("Ending Pre Log Insert"+str(vLoopSQLSplitCount));

                            print("Going Inside Split Array");

                            ########################################################
                            # ----  Each Script in GIT File May Have 1/many SQL Statements Start ------- #
                            # ---- Each Statement will be executed separately Start ------ #
                            ########################################################

                            for vSQLStatement in vListSQLStatements:

                                vLoopSQLSplitCount+=1;
                                #vParsedListSQLStatements=vParsedListSQLStatements+"\n"+vSQLStatement;
                                vParsedListSQLStatements=vSQLStatement;

                                ###########################################################
                                # ---- Execute Data Vault Script Execution Logic Block Start------- #
                                ###########################################################

                                try:

                                    vSQL=vParsedListSQLStatements;
                                    vSnowflakeCursorDataVault.execute(vSQL);

                                except:

                                    ###########################################################
                                    # ---- If any DV Script Fails, RollBack the Entire DV Transaction - Start------- #
                                    ###########################################################

                                    vSnowflakeConnectionDataVault.rollback();

                                    ###########################################################
                                    # ---- If any DV Script Fails, RollBack the Entire DV Transaction - End------- #
                                    ###########################################################

                                    vLoopStatus='2-Failed';
                                    vLoopExceptStatus=vLoopStatus;
                                    vLoopStatus='4-Unknown';

                                    vGlobalErrorMessage=traceback.format_exc();
                                    print("Exception Encountered");
                                    print(vGlobalErrorMessage);
                                    context.updateVariable('JobVarErrorMessage',vGlobalErrorMessage);

                                    vLoopCurrentTimestampStr=str(datetime.datetime.today()).split(' ')[0]+' '+str(datetime.datetime.today()).split(' ')[1].split('.')[0];
                                    vLoopScript=vNestedLoopPath+'/'+vItem['name'];

                                    print("Starting Fail Log Insert"+str(vLoopSQLSplitCount));

                                    ###########################################################
                                    # ---- Insert an entry in Log table for Failure - Start------- #
                                    ###########################################################

                                    vLoopStatus='2-Failed';

                                    vSQL= ("""
                                    INSERT INTO """+vLogSchema+""".LOG_DV_EXECUTION_CURR
                                    (SEQ_NO,MSTR_EXEC_HASHKEY,SUBJ_AREA,STAGE,EXEC_SCRIPT,EXEC_SCRIPT_SEQ,EXEC_SCRIPT_CONT,EXEC_TM,EXEC_STATUS,EXEC_ERR_MSG)
                                    VALUES
                                    (
                                        ?,
                                        ?,
                                        ?,
                                        ?,
                                        ?,
                                        ?,
                                        ?,
                                        ?,
                                        ?,
                                        ?
                                        )
                                    """);

                                    vSnowflakeCursorLog.execute(vSQL,[vCount,vMSTRHashKey,vLoopConfigSchemaList,vLoopConfigDVStagesList,vLoopScript,vLoopSQLSplitCount,vParsedListSQLStatements,vLoopCurrentTimestampStr,vLoopExceptStatus,vGlobalErrorMessage]);

                                    ###########################################################
                                    # ---- Insert an entry in Log table for Failure - End------- #
                                    ###########################################################

                                    ###########################################################
                                    # ---- If any DV Script Fails, Update the Log Master - Start------- #
                                    ###########################################################

                                    vSQL=("""UPDATE """+vLogSchema+""".LOG_DV_EXECUTION_MSTR
                                    SET
                                        EXEC_END_TM = current_timestamp,
                                        EXEC_STATUS = ?,
                                        EXEC_ERR_MSG = ?
                                    WHERE 1=1
                                    AND ID = ? """ );

                                    vSnowflakeCursorLog.execute(vSQL,[vLoopExceptStatus,vGlobalErrorMessage,vID]);

                                    ###########################################################
                                    # ---- If any DV Script Fails, Update the Log Master - End------- #
                                    ###########################################################

                                    print("Ending Fail Log Insert"+str(vLoopSQLSplitCount));

                                finally:

                                    ###########################################################
                                    # ---- If any DV Script Fails, Terminate The Process and Close Snowflake Connections - Start------- #
                                    ###########################################################

                                    if (vLoopExceptStatus == '2-Failed'):

                                        vSnowflakeConnectionLog.close();
                                        vSnowflakeCursorLog.close();

                                        vSnowflakeConnectionDataVault.close();
                                        vSnowflakeCursorDataVault.close();

                                        exit(0);

                                    ###########################################################
                                    # ---- If any DV Script Fails, Terminate The Process and Close Snowflake Connections - End------- #
                                    ###########################################################

                                ###########################################################
                                # ---- Execute Data Vault Script Execution Logic Block End------- #
                                ###########################################################

                                vParsedListSQLStatements='';
                                vLoopScript='';
                                vLoopCurrentTimestampStr='';


                            ########################################################
                            # ----  Each Script in GIT File May Have 1/many SQL Statements End ------- #
                            # ---- Each Statement will be executed separately End ------ #
                            ########################################################

                            vLoopCurrentTimestampStr=str(datetime.datetime.today()).split(' ')[0]+' '+str(datetime.datetime.today()).split(' ')[1].split('.')[0];
                            vLoopScript=vNestedLoopPath+'/'+vItem['name'];
                            vLoopStatus='3-Success';

                            print("Starting Success Log Insert");

                            ###########################################################
                            # ---- Insert an entry in Log table for Success of Each DV Script - Start------- #
                            ###########################################################

                            vSQL= ("""
                            INSERT INTO """+vLogSchema+""".LOG_DV_EXECUTION_CURR
                            (SEQ_NO,MSTR_EXEC_HASHKEY,SUBJ_AREA,STAGE,EXEC_SCRIPT,EXEC_SCRIPT_SEQ,EXEC_SCRIPT_CONT,EXEC_TM,EXEC_STATUS)
                            VALUES
                            (
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?
                            )
                            """);

                            vSnowflakeCursorLog.execute(vSQL,[vCount,vMSTRHashKey,vLoopConfigSchemaList,vLoopConfigDVStagesList,vLoopScript,0,vContent,vLoopCurrentTimestampStr,vLoopStatus]);

                            ###########################################################
                            # ---- Insert an entry in Log table for Success of Each DV Script - End------- #
                            ###########################################################

                            print("Ending Success Log Insert"+str(vLoopSQLSplitCount));


        ########################################################
        # ---- Loop though Functional Area & Branches End ------- #
        # ---- Functional Area & Branches will loop in sorted Manner to ensure Staging -> HLS -> Bus Vault ---- #
        ########################################################

        ###########################################################
        # ---- Once all DV Scripts are executed, Update The Log Master with Success - Start------- #
        ###########################################################

        vSQL=("""UPDATE """+vLogSchema+""".LOG_DV_EXECUTION_MSTR
        SET
            EXEC_END_TM = current_timestamp,
            EXEC_DRTN = TIMESTAMPDIFF('secs',EXEC_STRT_TM,current_timestamp),
            EXEC_STATUS = ?
        WHERE 1=1
        AND ID = ? """ );

        vSnowflakeCursorLog.execute(vSQL,['3-Success',vID]);


        ###########################################################
        # ---- Once all DV Scripts are executed, Update The Log Master with Success - End------- #
        ###########################################################

        ###########################################################
        # ---- Once everything is done, Success Message Generation - Start------- #
        ###########################################################

        vSQL="""SELECT
                    'Execution ID - '||EXEC_HASHKEY||CHAR(10)||
                    'Process Name - '||PROCESS_NM||CHAR(10)||
                    'Execution Date - '||TO_CHAR(EXEC_DT_KEY)||CHAR(10)||
                    'Execution Start Time - '||TO_CHAR(EXEC_STRT_TM)||CHAR(10)||
                    'Execution End Time - '||TO_CHAR(EXEC_END_TM)||CHAR(10)||
                    'Execution Status - '||TO_CHAR(EXEC_STATUS)||CHAR(10)
                        AS VAL
                FROM """ +vLogSchema+""".LOG_DV_EXECUTION_MSTR
                WHERE ID = ? """ ;

        vSnowflakeCursorLog.execute(vSQL,[vID]);

        vSuccessResultArr=vSnowflakeCursorLog.fetchone();
        vSuccessResult=vSuccessResultArr[0];

        vGlobalMessage=vSuccessResult;
        print(vGlobalMessage);
        context.updateVariable('JobVarSuccessMessage',vGlobalMessage);

        ###########################################################
        # ---- Once everything is done, Success Message Generation - End------- #
        ###########################################################

        ###########################################################
        # ---- Once everything is done, commit the DV Process - Start------- #
        ###########################################################

        vSnowflakeConnectionDataVault.commit();

        ###########################################################
        # ---- Once everything is done, commit the DV Process - End------- #
        ###########################################################

    except:

        ###########################################################
        # ---- Except the DV Execution Failure, Rest all the exception Handling Block - Start------- #
        ###########################################################

        if (vLoopStatus != '2-Failed'):

            if (vGlobalErrorMessage == ''):
            	vGlobalErrorMessage=traceback.format_exc();

            if (int(vID) > -1):

                vSQL=("""UPDATE """+vLogSchema+""".LOG_DV_EXECUTION_MSTR
                SET
                    EXEC_END_TM = current_timestamp,
                    EXEC_DRTN = TIMESTAMPDIFF('secs',EXEC_STRT_TM,current_timestamp),
                    EXEC_STATUS = '2-Failed',
                    EXEC_ERR_MSG = ?
                WHERE 1=1
                AND ID = ? """ );

                vSnowflakeCursorLog.execute(vSQL,[vGlobalErrorMessage,vID]);

            print("Exception Encountered");
            print(vGlobalErrorMessage);



            context.updateVariable('JobVarErrorMessage',vGlobalErrorMessage);

        ###########################################################
        # ---- Except the DV Execution Failure, Rest all the exception Handling Block - End------- #
        ###########################################################

    finally:

        if (vSnowflakeCursorLogFlag == True):

            vSnowflakeConnectionLog.close();
            vSnowflakeCursorLog.close();

        if (vSnowflakeConnectionDataVaultFlag == True):

            vSnowflakeConnectionDataVault.close();
            vSnowflakeCursorDataVault.close();

        exit(0);

if __name__=='__main__':

    global vUsername;
    global vPassword;
    global vAccount;
    global vWarehouse;
    global vDatabase;
    global vSchema;
    global vLogDatabase;
    global vLogSchema;

    global vToken;
    global vURL;
    global vProject;
    global vBranch;
    global vBusAreaFolder;
    global vStageFolder;

    global vGlobalErrorMessage;
    global vGlobalMessage;

    vUsername=JobVarCredUsername;
    vPassword=JobVarCredPswd;
    vAccount=JobVarCredAccount;
    vWarehouse=JobVarCredWH;
    vDatabase=JobVarCredDVDatabase;
    vSchema='';

    vLogDatabase=JobVarCredLogDatabase;
    vLogSchema=JobVarCredLogSchema;

    vToken=JobVarCredToken;
    vURL=JobVarCredURL;
    vProject=JobVarCredProject;
    vBranch=JobVarCredBranch;
    vBusAreaFolder=JobVarCredBusAreaFolder;
    vStageFolder=JobVarCredStageFolder;

    executeDVScripts();
