from datetime import datetime
import json
import os
import re
import mysql.connector as mysql
from mysql.connector import Error

db_name = 'JOB_Analyser_DB'

dir_path_Applied = "/Users/web_dev/Desktop/Applied_JOB/"
tbl_name_Job_detail = 'JOB_Analyser_App_jobdetail'

dir_path_for_Job_Status_Later = "/Users/web_dev/Desktop/JOB_Status/"
tbl_name_Job_Status_Later = 'JOB_Analyser_App_jobstatuslater'

insert_query_for_Job_Status_Later = '''INSERT INTO 
JOB_Analyser_App_jobstatuslater(company,designation,status,statusid,source,created)
  VALUES (%s,%s,%s,%s,%s,%s)'''

# delete_Job_Status_Later_query = ''' delete t1 FROM JOB_Analyser_App_jobstatuslater t1
# INNER  JOIN JOB_Analyser_App_jobstatuslater t2
# WHERE t1.id < t2.id AND
#     t1.company = t2.company AND
#     t1.designation = t2.designation AND
#     t1.statusid = t2.statusid AND
#     t1.source = t2.source
#     ;
# '''
delete_Job_Status_Later_query = ''' delete t1 FROM JOB_Analyser_App_jobstatuslater t1
INNER  JOIN JOB_Analyser_App_jobstatuslater t2
WHERE t1.id < t2.id AND
    t1.company = t2.company AND
    t1.designation = t2.designation AND
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


def saving_in_db(cursor1, query, query_values, txt_task):
    try:
        cursor1.execute(query, query_values)
        db.commit()
        print(txt_task + " Database Successfully")
        return True
    except mysql.Error as err:
        print("MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))


def updating_post_in_db(cursor12, query, txt_task):
    try:
        cursor12.execute(query)
        result = cursor12.rowcount
        db.commit()

        if result > 0:
            print(str(result) + " Rows affected")
            print(txt_task + " Database Successfully")
            return True

        return False
    except mysql.Error as err:
        print(txt_task + " MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))


def updating_in_db(cursor11, query, txt_task):
    try:
        cursor11.execute(query)
        # result = cursor11.rowcount
        db.commit()

        # if result > 0:
        #     print(str(result) + " Rows affected")
        #     print(txt_task + " Database Successfully")
        #     return True
        print(txt_task + " Database Successfully")
        return False
    except mysql.Error as err:
        print(txt_task + " MySql Exception: {}".format(err))
        return False
    except Exception as eee:
        print("Exception: {}".format(eee))


def update_To_Reject_job_detail_record(cursor11):
    try:
        update_to_reject_query_for_job_detail = "UPDATE JOB_Analyser_DB.JOB_Analyser_App_jobdetail SET status_id_id = 7  WHERE  created < (curdate() - interval 10 day) AND status_id_id <=2";

        # print(update_toreject_query_for_job_detail)
        updating_in_db(cursor11, update_to_reject_query_for_job_detail, 'Updated Status to Reject ')

    except Exception as eee:
        print("General Exception in METHOD update_To_Reject_job_detail_record filename  " + str(eee))


def update_job_detail_record(cursor1, arr_json_data, file_name):
    for r_data in arr_json_data:
        try:
            company = r_data['Company'].strip()
            designation = r_data['Designation'].strip()
            if (company == "") or (designation == ""):
                print("Company or Designation is empty")
                return False
            statusid = str(r_data['StatusID'])

            # update_query_for_job_detail = "UPDATE JOB_Analyser_DB.JOB_Analyser_App_jobdetail SET status_id_id = " + statusid + " WHERE company = '" + company + "' AND designation = '" + designation + "' AND status_id_id < " + statusid + " AND created > (curdate() - interval 25 day)"

            update_query_for_job_detail = "UPDATE JOB_Analyser_DB.JOB_Analyser_App_jobdetail SET status_id_id = " + statusid + " WHERE company = '" + company + "' AND designation = '" + designation + "' AND created > (curdate() - interval 25 day) AND status_id_id < " + statusid

            # print(update_query_for_job_detail)
            if updating_in_db(cursor1, update_query_for_job_detail, 'Updated Status in'):
                print("")

        except Exception as eee:
            print("General Exception in METHOD update_job_detail_record filename " + file_name + " " + str(eee))
        # return False


def calculate_Possibility(postedtime, source):
    postedtimeee = postedtime.lower()
    try:
        if source.lower().strip() == "glassdoor":
            if postedtimeee.endswith('d'):
                return 0
            if postedtimeee.endswith('h'):
                return 50

        arr_str_right_time = ["minutes", "now"]
        arr_str_delay = ["month", "day", "today"]

        for ttime in arr_str_right_time:
            if ttime in postedtimeee:
                return 100

        for ttime in arr_str_delay:
            if ttime in postedtimeee:
                return 0

        if "hour" in postedtimeee:
            arr_num_hour = re.findall(r'\d+', postedtimeee)
            if len(arr_num_hour) > 0:
                hour_only = int(arr_num_hour[0])
                if hour_only <= 2:
                    return 100
                elif hour_only <= 4:
                    return 50
                else:
                    return 0
        return 0
    except Exception as eee:
        print("General Exception in METHOD calculate_Possibility  " + " " + str(
            eee))
        return 0


def update_job_posted_job_detail_record(cursor1, json_data, file_name):
    try:
        company = json_data['Company'].strip()
        designation = json_data['Designation'].strip()
        source = json_data['Source'].strip()
        posted_timee = json_data['Posted_DateTime'].strip()
        posted_timee = posted_timee.lower().replace("posted", "")

        str_possibility = str(calculate_Possibility(posted_timee, source))

        if "ago" not in posted_timee:
            posted_timee = posted_timee + " ago"

        if (company == "") or (designation == "") or (posted_timee == ""):
            print("Company or Designation or postedTime is empty")
            return False

        update_job_posted_job_detail_record = "UPDATE JOB_Analyser_DB.JOB_Analyser_App_jobdetail SET possibility = "+str_possibility+", posted_time = '" + posted_timee + "' WHERE  source ='" + source + "' AND  company = '" + company + "' AND designation = '" + designation + "' AND created > (curdate() - interval 5 day)"

        print(update_job_posted_job_detail_record)

        if updating_post_in_db(cursor1, update_job_posted_job_detail_record, 'Updated PostedTime in'):
            return True
        return False

    except Exception as eee:
        print("General Exception in METHOD update_job_posted_job_detail_record filename " + file_name + " " + str(
            eee))
    return False


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

            saving_in_db(cursor1, insert_query_for_Job_Status_Later, jb_status_later, 'Saved')

        except Exception as eee:
            print("General Exception in filename " + file_name + " " + str(eee))
            # return False

    return True


def read_job_status_later_jsonForUpdate(cursor11):
    try:
        # print(dir_path)
        for file_name in [file for file in os.listdir(dir_path_for_Job_Status_Later) if
                          (file.endswith('.json') and not (file.__contains__('done_')))]:
            path_with_file_name = dir_path_for_Job_Status_Later + file_name
            try:
                with open(path_with_file_name) as json_file:
                    data = json.load(json_file)

                    update_job_detail_record(cursor11, data, file_name)

            except EnvironmentError:
                print('EnvironmentError Exception')
                continue
    except FileNotFoundError:
        print("Error: FileNotFoundError")
    except EOFError:
        print("Exception")
    except Exception:
        print('General Exception in function name: read_job_status_later_jsonForUpdate  ' + str(Exception))
        print(Exception)


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


def read_job_posted_json_For_Update(cursor11):
    try:
        # print(dir_path)
        for file_name in [file for file in os.listdir(dir_path_Applied) if
                          (file.startswith('Posted') and file.endswith('.json') and not (file.__contains__('done_')))]:
            path_with_file_name = dir_path_Applied + file_name
            try:
                with open(path_with_file_name) as json_file:
                    data = json.load(json_file)

                    shall_i_proceed = update_job_posted_job_detail_record(cursor11, data, file_name)
                    if shall_i_proceed:
                        new_file_name_with_path = dir_path_Applied + 'done_' + file_name
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
        print('General Exception in function name: read_job_status_later_jsonForUpdate  ' + str(Exception))
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
    read_job_status_later_jsonForUpdate(cursor)
    read_job_status_later_json(cursor)
    update_To_Reject_job_detail_record(cursor)
    delete_duplicate_records(cursor)
    db.close()

    db = mysql_connect()
    # db.execute('set max_allowed_packet=67108864')
    # cursor1 = db.cursor(buffered=True)
    cursor1 = db.cursor(buffered=True)
    read_job_posted_json_For_Update(cursor1)
    db.close()
