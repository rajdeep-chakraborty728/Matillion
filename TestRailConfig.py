
# API Column List For Various Objects
SuiteColumns = [
            'id',
            'name',
            'project_id',
            'is_master',
            'is_baseline',
            'is_completed',
            'completed_on',
            'url'
        ];

CaseColumns = [
    'id',
    'title',
    'section_id',
    'type_id',
    'priority_id',
    'created_on',
    'updated_on',
    'suite_id',
    'milestone_id',
    'is_frontend_tc',
    'custom_automation_status',
    'custom_added_in_release',
    'custom_is_regression',
    'custom_squad_name',
    'custom_automation_owner',
    'custom_tc_added_to_train',
    'custom_automation_target_date',
    'custom_automated_test_case_name',
    'custom_customer_found',
    'custom_is_dmaas',
    'display_order',
    'estimate',
    'estimate_forecast',
    'is_deleted',
    'custom_customer_found_defect_id',
    'custom_automation_type'
];

MilestoneColumns=[
    'id',
    'name',
    'description',
    'start_on',
    'started_on',
    'is_started',
    'due_on',
    'is_completed',
    'completed_on',
    'project_id',
    'parent_id',
    'refs',
    'url',
    'milestones'
];

#Cohesity Project ID For Testrail Integration
ProjectId=4;

#Test Rail Public Configurations
CohesityTestRailBaseURL='https://cohesity.testrail.com/index.php?/api/v2/';
APILimit=250;
TestRailEPochDate=1000000000;

#No Of Suites to Fetch Data from Testrail
ConfigNoOfSuites=2000;


# Snowflake Table Names
SnowflakeSuiteTable='TESTRAIL_SUITES';
SnowflakeCaseTable='TESTRAIL_CASES';


# Snowflake Object Mapping
SnowflakeObjectMapping={
                        'Suites': [1,'TESTRAIL_SUITES'],
                        'Cases': [2,'TESTRAIL_CASES','UPDATED_ON'],
                        'Milestones' : [3,'TESTRAIL_MILESTONES'],
                        'HighWatermark':['MATILLION_TEMP','TESTRAIL_HIGHWATERMARK']
                        };
