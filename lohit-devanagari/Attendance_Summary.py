import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
from pandas.io.json import json_normalize
from functools import reduce
from scipy import stats
from flask import Flask
import json
import logging
#import decode
import datetime
import time
from wordcloud import WordCloud, STOPWORDS
import gridfs
import matplotlib.pyplot as plt
from datetime import datetime
import atexit
from apscheduler.schedulers.blocking import BlockingScheduler
#from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger


client = MongoClient('mongodb://localhost:27017/')
db = client['pdea_pilot']

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',filename='/home/ec2-user/Shekhar/Pooja/Final_Analysis_Json_Files/Attendance_Summary.log',
                    filemode='a')

def Attendance_Analysis_BranchLevel():

    test = db.Attendance
    attend_list = list(test.find({}))
    attend_df = pd.DataFrame(json_normalize(attend_list))
        #############################

    test1 = db.SchoolBranches
    schoolbranches_list = list(test1.find({}))

    schoolbranches_df = pd.DataFrame(json_normalize(schoolbranches_list,'Classes',['_id','Location','Principal',
                                                                                       'Address1','Address2','BranchName',
                                                                                       'Area','City','State','Country','PostalCode','PhoneNo1','PhoneNo2','PhoneNo3','MobileNo1','MobileNo2',
                                                                                              'MobileNo3','SchoolEmailID','WebsiteUrl','Houses',['School','SchoolID'],['School','SchoolName'],'__v'],errors='ignore',record_prefix='Class'))

    schoolbranches_df_required = schoolbranches_df[['School.SchoolID','School.SchoolName','_id', 'BranchName']].copy()

    schoolbranches_df_required = schoolbranches_df_required.drop_duplicates()


    attend_status = attend_df.groupby(['BranchID', 'Date', (attend_df['AttendanceStatus'] == 'P')])['_id'].count().reset_index()

    present = attend_status[attend_status['AttendanceStatus'] == True]
    present.columns = ['BranchID', 'Date', 'AttendanceStatus', 'Present_Count']
    absent = attend_status[attend_status['AttendanceStatus'] == False]
    absent.columns = ['BranchID', 'Date', 'AttendanceStatus', 'Absent_Count']

    final_attend_status = pd.merge(present, absent, how='inner', left_on=(['BranchID', 'Date']),right_on=(['BranchID', 'Date']))
    final_attend_status_required = final_attend_status[['BranchID', 'Date', 'Present_Count', 'Absent_Count']].copy()

    Attendance_Df = pd.merge(final_attend_status_required, schoolbranches_df_required, how='inner',left_on=(['BranchID']), right_on=(['_id']))

    Attendance_Df_required = Attendance_Df[['School.SchoolID','School.SchoolName','BranchID', 'BranchName', 'Date', 'Present_Count', 'Absent_Count']].copy()
    Attendance_Df_required.columns=['SchoolID','SchoolName','BranchID', 'BranchName', 'Date', 'Present_Count', 'Absent_Count']

    #Attendance_Df_required['SchoolID'] = Attendance_Df_required['SchoolID'].astype('str')
    #Attendance_Df_required['BranchID'] = Attendance_Df_required['BranchID'].astype('str')
    #Attendance_Df_required['Date'] = Attendance_Df_required['Date'].astype('str')
    #Attendance_Df_required['RunDate'] = str(datetime.date(datetime.now()))

    #Attendance_Df_required.to_json('Final_Analysis_Json_Files/Attendance_Analysis.json', orient='records')

    db.Attendance_Analysis.drop()
    #record1 = db.Attendance_Analysis
    #page = open('Final_Analysis_Json_Files/Attendance_Analysis.json', 'r')
    #parsed = json.loads(page.read().encode('utf-8'))
    ## print(parsed['Records'])
    #for item in parsed:
    #    record1.insert_one(item)

    result1 = Attendance_Df_required.to_dict(orient='records')
    db.Attendance_Analysis.insert_many(result1)


    logging.debug("Data inserted in Attendance_Analysis for branch level")
    return ('Attendance Added in Attendance_Analysis for branch level')


def Attendance_Analysis_ClassLevel():

    # Comparative Analysis Attendance

    # Collection Used: Attendance, Students, SchoolBranches

    attend = list(db.Attendance.find({}))
    attend_df = pd.DataFrame(json_normalize(attend))

    stud = list(db.Students.find({}))
    stud_df = pd.DataFrame(json_normalize(stud))
    stud_df_required = stud_df[['_id', 'BranchID', 'Class.ClassID', 'Class.Standard', 'Class.Section']].copy()
    stud_df_required.columns = ['StudentID', 'BranchID', 'ClassID', 'ClassStandard', 'ClassSection']

    schoolbranches_list = list(db.SchoolBranches.find({}))
    schoolbranches_df = pd.DataFrame(json_normalize(schoolbranches_list, 'Classes', ['_id', 'Location', 'Principal',
                                                                                     'Address1', 'Address2',
                                                                                     'BranchName',
                                                                                     'Area', 'City', 'State', 'Country',
                                                                                     'PostalCode', 'PhoneNo1',
                                                                                     'PhoneNo2', 'PhoneNo3',
                                                                                     'MobileNo1', 'MobileNo2',
                                                                                     'MobileNo3', 'SchoolEmailID',
                                                                                     'WebsiteUrl', 'Houses',
                                                                                     ['School', 'SchoolID'],
                                                                                     ['School', 'SchoolName'], '__v'],
                                                    errors='ignore', record_prefix='Class'))

    schoolbranches_df_required = schoolbranches_df[['School.SchoolID', 'School.SchoolName', '_id', 'BranchName']].copy()

    schoolbranches_df_required = schoolbranches_df_required.drop_duplicates()
    schoolbranches_df_required.columns = ['SchoolID', 'SchoolName', 'BranchID', 'BranchName']

    #####################################################
    # Merge attend_df and stud_df_required

    required_df = pd.merge(stud_df_required, attend_df, how='inner', left_on=['BranchID', 'StudentID'],
                           right_on=['BranchID', 'StudentID'])

    #print(required_df.info())
    attend_status = required_df.groupby(['BranchID', 'ClassID_x', 'ClassStandard', 'ClassSection', 'Date',
                                         (required_df['AttendanceStatus'].isin(['P', 'present']))])['_id'].count().reset_index()

    present = attend_status[attend_status['AttendanceStatus'] == True]
    present.columns = ['BranchID', 'ClassID', 'ClassStandard', 'ClassSection', 'Date', 'AttendanceStatus',
                       'Present_Count']
    absent = attend_status[attend_status['AttendanceStatus'] == False]
    absent.columns = ['BranchID', 'ClassID', 'ClassStandard', 'ClassSection', 'Date', 'AttendanceStatus',
                      'Absent_Count']

    final_attend_status = pd.merge(present, absent, how='inner',
                                   left_on=(['BranchID', 'ClassID', 'ClassStandard', 'ClassSection', 'Date']),
                                   right_on=(['BranchID', 'ClassID', 'ClassStandard', 'ClassSection', 'Date']))
    final_attend_status_required = final_attend_status[
        ['BranchID', 'ClassID', 'ClassStandard', 'ClassSection', 'Date', 'Present_Count', 'Absent_Count']].copy()

    ############################################################
    '''merge final_attend_status_required  with schoolbranches_df_required to get school id as FAS(final attendance status)'''
    FAS = pd.merge(schoolbranches_df_required, final_attend_status_required, how='inner', left_on=['BranchID'],
                   right_on=['BranchID']).reset_index().sort_values(['BranchID', 'Date'])
    FAS.drop(['index'], axis=1, inplace=True)

    ############################################################################
    #FAS['SchoolID'] = FAS['SchoolID'].astype('str')
    #FAS['BranchID'] = FAS['BranchID'].astype('str')
    #FAS['ClassID'] = FAS['ClassID'].astype('str')
    #FAS['Date'] = FAS['Date'].astype('str')

    #FAS.to_json('Final_Analysis_Json_Files/Attendance_Analysis_ClassLevel.json', orient='records')

    db.Attendance_Analysis_ClassLevel.drop()
    #record1 = db.Attendance_Analysis_ClassLevel
    #page = open('Final_Analysis_Json_Files/Attendance_Analysis_ClassLevel.json', 'r')
    #parsed = json.loads(page.read().encode('utf-8'))
    ## print(parsed['Records'])
    #for item in parsed:
    #    record1.insert_one(item)

    result2 = FAS.to_dict(orient='records')
    #print(result2)
    db.Attendance_Analysis_ClassLevel.insert_many(result2)

    logging.debug("Data inserted in Attendance_Analysis_ClassLevel")
    return ('Attendance Added in in Attendance_Analysis_ClassLevel')

def Summary_Report_df():
    #print('In Summary')
    test1 = db.Students
    stud_df_list = list(test1.find({}))
    students_df = pd.DataFrame(json_normalize(stud_df_list))
    students_required_df = students_df[
        ['BranchID', 'Class.ClassID', 'Class.Standard', 'Class.Section', 'Gender', '_id']].copy()

    test2 = db.SchoolBranches
    schoolbranches_list = list(test2.find({}))
    schoolbranches_df = pd.DataFrame(json_normalize(schoolbranches_list))
    schoolbranches_required_df = schoolbranches_df[['_id', 'BranchName', 'School.SchoolID', 'School.SchoolName']].copy()

    student_schoolbranch_df = pd.merge(students_required_df, schoolbranches_required_df, how='inner',
                                       left_on=['BranchID'], right_on=['_id'])

    # Here _id_x is studentid
    ##############################################################
    '''Branch Level Student info calculation'''

    Branch_Total_students = \
        student_schoolbranch_df.groupby(['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName'])[
            '_id_x'].count().reset_index()
    Branch_Total_students.columns = ['SchoolID', 'SchoolName', 'BranchID', 'BranchName', 'Total_Students']
    #################################################################

    Branch_Boys_count = \
    student_schoolbranch_df.groupby(['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName',
                                     ((student_schoolbranch_df.Gender.isin(['Male', 'MALE', 'M'])))])[
        '_id_x'].count().reset_index()
    Branch_Boys_count.columns = ['SchoolID', 'SchoolName', 'BranchID', 'BranchName', 'Gender', "Branch_Boys_count"]
    Branch_Boys_count = Branch_Boys_count[Branch_Boys_count['Gender'] == True]
    ######################################################################

    Branch_Girls_count = \
    student_schoolbranch_df.groupby(['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName',
                                     ((student_schoolbranch_df.Gender.isin(['Female', 'FEMALE', 'F'])))])[
        '_id_x'].count().reset_index()
    Branch_Girls_count.columns = ['SchoolID', 'SchoolName', 'BranchID', 'BranchName', 'Gender', "Branch_Girls_count"]
    Branch_Girls_count = Branch_Girls_count[Branch_Girls_count['Gender'] == True]
    ######################################################################
    Dfs = [Branch_Total_students, Branch_Boys_count, Branch_Girls_count]

    branch_df = reduce(lambda left, right: pd.merge(left, right, on=['SchoolID', 'SchoolName', 'BranchID',
                                                                     'BranchName'], how='outer'), Dfs).fillna(0)
    branch_df_required = branch_df[
        ['SchoolID', 'SchoolName', 'BranchID', 'BranchName', 'Total_Students', 'Branch_Boys_count',
         'Branch_Girls_count']]

    ################################################################################

    ################################################################
    '''Class Level Student info calculation'''

    classcount = student_schoolbranch_df.groupby(['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName',
                                                  'Class.ClassID', 'Class.Standard', 'Class.Section'])[
        '_id_x'].count().reset_index()

    classcount['Class'] = classcount[['Class.Standard', 'Class.Section']].astype(str).apply(lambda x: ' '.join(x),
                                                                                            axis=1)

    ###############################################################################
    Class_Boys_count = \
    student_schoolbranch_df.groupby(['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName',
                                     'Class.ClassID', 'Class.Standard', 'Class.Section',
                                     ((student_schoolbranch_df.Gender.isin(['Male', 'MALE', 'M'])))])[
        '_id_x'].count().reset_index()

    Class_Boys_count.columns = ['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName', 'Class.ClassID',
                                'Class.Standard', 'Class.Section',
                                'Gender', "Class_Boys_count"]
    Class_Boys_count = Class_Boys_count[Class_Boys_count['Gender'] == True]

    ###########################################################################

    Class_Girls_count = \
    student_schoolbranch_df.groupby(['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName',
                                     'Class.ClassID', 'Class.Standard', 'Class.Section',
                                     ((student_schoolbranch_df.Gender.isin(['Female', 'FEMALE', 'F'])))])[
        '_id_x'].count().reset_index()

    Class_Girls_count.columns = ['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName', 'Class.ClassID',
                                 'Class.Standard', 'Class.Section',
                                 'Gender', "Class_Girls_count"]
    Class_Girls_count = Class_Girls_count[Class_Girls_count['Gender'] == True]

    ##################################################
    ##Merge Class Results

    Dfs_collection = [classcount, Class_Boys_count, Class_Girls_count]
    class_df = reduce(lambda left, right: pd.merge(left, right, on=['School.SchoolID', 'School.SchoolName', 'BranchID',
                                                                    'BranchName', 'Class.ClassID', 'Class.Standard',
                                                                    'Class.Section'], how='outer'),
                      Dfs_collection).fillna(0)
    class_df_required = class_df[['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName', 'Class.ClassID',
                                  'Class.Standard', 'Class.Section', '_id_x', 'Class', 'Class_Boys_count',
                                  'Class_Girls_count']].copy()

    # ######################################################
    '''Class Details Calculations'''
    d = {}
    for i in class_df_required['BranchID'].unique():
        d[i] = [{"ClassID": str(class_df_required['Class.ClassID'][j]),
                 "Class_Name": class_df_required['Class'][j],
                 "Class_Count": class_df_required['_id_x'][j],
                 "Class_Boys_Count": class_df_required['Class_Boys_count'][j],
                 "Class_Girls_Count": class_df_required['Class_Girls_count'][j]} for j in
                class_df_required[class_df_required['BranchID'] == i].index]

    final_class_df = pd.DataFrame(columns=['BranchID', 'Class_Details'])
    for key, value in d.items():
        final_class_df = final_class_df.append({'BranchID': key, 'Class_Details': value}, ignore_index=True)

    ###########################################################################################################
    '''BranchLevel Teacher Calculation'''
    test4 = db.Users
    users_df_list = list(test4.find({}))
    users_df = pd.DataFrame(json_normalize(users_df_list, 'Roles',
                                           ['_id', 'SchoolID', 'FirstName', 'MiddleName', 'LastName', 'Address1',
                                            'Address2', 'Area', 'City', 'State', 'Country', 'PostalCode', 'PhoneNo1',
                                            'PhoneNo2', 'MobileNo1', 'MobileNo2', 'PrimaryEmailID', 'AlternateEmailID',
                                            'UserName', 'Password', 'BranchID'], errors='ignore'))
    user_required_df = users_df[['_id', 'BranchID', 'RoleId', 'RoleName', 'FirstName', 'LastName']].copy()

    user_schoolbranch_df = pd.merge(user_required_df, schoolbranches_required_df, how='inner', left_on=['BranchID'],
                                    right_on=['_id'])
    # #user_schoolbranch_df['TeacherName'] = user_schoolbranch_df[['FirstName', 'LastName']].apply(lambda x: " ".join(x),
    #                                                                                                 axis=1)
    Teacher_Branch_count_df = user_schoolbranch_df.groupby(
        ['School.SchoolID', 'School.SchoolName', 'BranchID', 'BranchName',
         (user_schoolbranch_df.RoleName.isin(['SubjectTeacher', 'ClassTeacher']))])[
        '_id_x'].count().reset_index()
    Final_Teacher_Branch_count_df = Teacher_Branch_count_df[Teacher_Branch_count_df['RoleName'] == True]
    Final_Teacher_Branch_count_df_required = Final_Teacher_Branch_count_df[['School.SchoolID', 'School.SchoolName',
                                                                            'BranchID', 'BranchName', '_id_x']]
    Final_Teacher_Branch_count_df_required.columns = ['SchoolID', 'SchoolName', 'BranchID', 'BranchName',
                                                      'Total_Teacher_Branch_Count']

    #####################################################################

    '''Final Calculation for Branch Teacher,Students and Class Students
    Merge Final_Teacher_Branch_count_df_required, branch_df_required and final_class_df'''
    Df_collection = [Final_Teacher_Branch_count_df_required, branch_df_required]

    summary_report = reduce(lambda left, right: pd.merge(left, right, on=['SchoolID', 'SchoolName', 'BranchID',
                                                                          'BranchName'], how='outer'),
                            Df_collection).fillna(0)

    final_summary_report = pd.merge(summary_report, final_class_df, how='outer', left_on=['BranchID'],
                                    right_on=['BranchID'])

    #####################################################
    final_summary_report['SchoolID'] = final_summary_report['SchoolID'].astype('str')
    final_summary_report['BranchID'] = final_summary_report['BranchID'].astype('str')

    final_summary_report.to_json('/home/ec2-user/Shekhar/Pooja/Final_Analysis_Json_Files/Summary_Report.json', orient='records')

    db.Summary_Report.drop()
    record1 = db.Summary_Report
    page = open('/home/ec2-user/Shekhar/Pooja/Final_Analysis_Json_Files/Summary_Report.json', 'r')
    parsed = json.loads(page.read().encode('utf-8'))

    for item in parsed:
        record1.insert_one(item)

    logging.debug('Data inserted in Summary_Report_json')
    return ('Summary_Report_json created')

Attendance_Analysis_BranchLevel()
Attendance_Analysis_ClassLevel()
Summary_Report_df()
