from datetime import datetime
import json
import os
import re
import mysql.connector as mysql
from mysql.connector import Error

dir_path_for_Job_Status_Later = "/Users/web_dev/Desktop/JOB_Status/"
db_name = 'JOB_Analyser_DB'
tbl_name_Job_Status_Later = 'JOB_Analyser_App_jobstatuslater'

insert_query_for_Job_Status_Later = '''INSERT INTO 
JOB_Analyser_App_jobstatuslater(company,designation,status,statusid,source,created)
  VALUES (%s,%s,%s,%s,%s,%s)'''

delete_Job_Status_Later_query = ''' delete t1 FROM JOB_Analyser_App_jobstatuslater t1
INNER  JOIN JOB_Analyser_App_jobstatuslater t2
WHERE t1.id < t2.id AND
    t1.company = t2.company AND
    t1.designation = t2.designation AND
    t1.statusid = t2.statusid AND 
    t1.source = t2.source
    ;
'''


def delete_duplicate_records(cursor12):
    try:
        cursor12.execute(delete_Job_Status_Later_query)
        db.commit()
        print("Successfully Deleted Duplicate records in Database")
        return True
    except mysql.Error as err:
        print("MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))
        return False


def saving_in_db(cursor1, query, query_values):
    try:
        cursor1.execute(query, query_values)
        db.commit()
        print("Saved in Database Successfully")
        return True
    except mysql.Error as err:
        print("MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))
        # return False


def add_new_job_status_later_record(cursor1, arr_json_data, file_name):
    for r_data in arr_json_data:
        try:
            company = r_data['Company'].strip()
            designation = r_data['Designation'].strip()
            if (company == "") or (designation == ""):
                print("Company or Designation is empty")
                return False
            created = datetime.now()
            jb_status_later = (r_data['Company'], r_data['Designation'], r_data['Status'], r_data['StatusID']
                               , r_data['Source'], created)
            print(jb_status_later)
            # if not saving_in_db(cursor1, insert_query_for_Job_Status_Later, jb_status_later):
            #     return False

            saving_in_db(cursor1, insert_query_for_Job_Status_Later, jb_status_later)

        except Exception as eee:
            print("General Exception in filename " + file_name + " " + str(eee))
            # return False

    return True


def read_job_status_later_json(cursor11):
    try:
        # print(dir_path)
        for file_name in [file for file in os.listdir(dir_path_for_Job_Status_Later) if
                          (file.endswith('.json') and not (file.__contains__('done_')))]:
            path_with_file_name = dir_path_for_Job_Status_Later + file_name
            # print(path_with_file_name)
            try:
                with open(path_with_file_name) as json_file:
                    data = json.load(json_file)
                    # print(data)
                    shall_i_proceed = add_new_job_status_later_record(cursor11, data, file_name)

                    if shall_i_proceed:
                        new_file_name_with_path = dir_path_for_Job_Status_Later + 'done_' + file_name
                        os.rename(path_with_file_name, new_file_name_with_path)
                    else:
                        continue

            except EnvironmentError:
                print('EnvironmentError Exception')
                continue
    except FileNotFoundError:
        print("Error: FileNotFoundError")
    except EOFError:
        print("Exception")
    except Exception:
        print('General Exception in function name: read_job_status_later_json  ' + str(Exception))
        print(Exception)


def mysql_connect():
    try:
        return mysql.connect(
            host='localhost',
            user='usrvijay',
            password='Nokia@123',
            database=db_name
        )
    except Error as e:
        print(e)
        return


if __name__ == '__main__':
    db = mysql_connect()
    cursor = db.cursor()
    delete_duplicate_records(cursor)
    read_job_status_later_json(cursor)
    delete_duplicate_records(cursor)
    db.close()
