from flask import Flask, render_template, request
from pymysql import connections
import os
import boto3
from config import *

app = Flask(__name__)

bucket = custombucket
region = customregion

db_conn = connections.Connection(
    host=customhost,
    port=3306,
    user=customuser,
    password=custompass,
    db=customdb

)
output = {}
table = 'employee'


@app.route("/", methods=['GET', 'POST'])
def home():
    return render_template('index.html')


@app.route("/employee")
def employee():
    return render_template('employeehome.html')


@app.route("/addemp", methods=['POST'])
def AddEmp():
    emp_id = request.form['emp_id']
    first_name = request.form['first_name']
    last_name = request.form['last_name']
    pri_skill = request.form['pri_skill']
    location = request.form['location']
    salary = request.form['salary']
    emp_image_file = request.files['emp_image_file']

    select_sql = "SELECT COUNT(*) FROM employee WHERE emp_id = %s"
    insert_sql = "INSERT INTO employee VALUES (%s, %s, %s, %s, %s, %s)"
    cursor = db_conn.cursor()

    if emp_image_file.filename == "":
        return "Please select a file"

    try:
        # Check if emp_id already exists in the database
        cursor.execute(select_sql, (emp_id,))
        result = cursor.fetchone()

        if result[0] > 0:
            return "Employee ID already exists. Please enter a different ID"
        
        # Validate salary as a number
        try:
            salary = float(salary)
        except ValueError:
            return "Salary must be a number"

        cursor.execute(insert_sql, (emp_id, first_name, last_name, pri_skill, location, salary))
        db_conn.commit()
        emp_name = "" + first_name + " " + last_name

        # Upload image file to S3
        emp_image_file_name_in_s3 = "emp-id-" + str(emp_id) + "_image_file"
        s3 = boto3.resource('s3')

        try:
            print("Data inserted in MySQL RDS... uploading image to S3...")
            s3.Bucket(custombucket).put_object(Key=emp_image_file_name_in_s3, Body=emp_image_file)
            bucket_location = boto3.client('s3').get_bucket_location(Bucket=custombucket)
            s3_location = (bucket_location['LocationConstraint'])

            if s3_location is None:
                s3_location = ''
            else:
                s3_location = '-' + s3_location

            object_url = "https://s3{0}.amazonaws.com/{1}/{2}".format(
                s3_location,
                custombucket,
                emp_image_file_name_in_s3)

        except Exception as e:
            return str(e)

    finally:
        cursor.close()

    print("All modifications done...")
    return render_template('AddEmpOutput.html', name=emp_name)


@app.route("/UpEmp", methods=['POST'])
def UpEmp():
    emp_id = request.form['emp_id']
    first_name = request.form['first_name']
    last_name = request.form['last_name']
    pri_skill = request.form['pri_skill']
    location = request.form['location']
    salary = request.form['salary']
    emp_image_file = request.files['emp_image_file']

    select_sql = "SELECT COUNT(*) FROM employee WHERE emp_id = %s"
    edit_sql = "UPDATE employee SET first_name=%s, last_name=%s, pri_skill=%s, location=%s, salary=%s WHERE emp_id=%s"
    cursor = db_conn.cursor()

    try:
        cursor.execute(select_sql, (emp_id,))
        result = cursor.fetchone()

        if result[0] == 0:
            return "Employee ID not exists, Please enter a different ID"
        
        try:
            salary = float(salary)
        except ValueError:
            return "Salary must be a number"

        cursor.execute(edit_sql, (first_name, last_name, pri_skill, location, salary, emp_id))
        db_conn.commit()
        emp_name = "" + first_name + " " + last_name

        if emp_image_file.filename != "":
            key = "emp-id-" + str(emp_id) + "_image_file.png"
            emp_image_file_name_in_s3 = "emp-id-" + str(emp_id) + "_image_file.png"
            s3 = boto3.resource('s3')

            try:
                print("Data inserted in MySQL RDS... uploading image to S3...")
                s3.Bucket(custombucket).put_object(Key=emp_image_file_name_in_s3, Body=emp_image_file)
                bucket_location = boto3.client('s3').get_bucket_location(Bucket=custombucket)
                s3_location = (bucket_location['LocationConstraint'])

                if s3_location is None:
                    s3_location = ''
                else:
                    s3_location = '-' + s3_location

                object_url = "https://s3{0}.amazonaws.com/{1}/{2}".format(
                    s3_location,
                    custombucket,
                    emp_image_file_name_in_s3)

            except Exception as e:
                return str(e)

    finally:
        cursor.close()

    print("all modification done...")
    return render_template('UpdateEmpOutput.html', name=emp_name, id=emp_id)

@app.route("/fetchdata", methods=['GET', 'POST'])
def FetchData():
    emp_id = request.form['emp_id']
    select_sql = "SELECT COUNT(*) FROM employee WHERE emp_id = %s"
    sqlCmd = "SELECT * FROM employee WHERE emp_id=%s"
    cursor = db_conn.cursor()

    if emp_id == "":
        return "Please enter an employee ID"

    url = None  # Initialize url with a default value

    try:
        cursor.execute(select_sql, ((emp_id,),))
        result = cursor.fetchone()

        if result[0] == 0:
            return "Employee ID not exists, Please enter a different ID"

        # Getting Employee Data
        cursor.execute(sqlCmd, ((emp_id,),))
        row = cursor.fetchone()
        dEmpID = row[0]
        dFirstName = row[1]
        dLastName = row[2]
        dPriSkill = row[3]
        dLocation = row[4]
        dSalary = row[5]

        key = "emp-id-" + str(emp_id) + "_image_file.png"

        s3_client = boto3.client('s3')
        for item in s3_client.list_objects(Bucket=custombucket)['Contents']:
            if item['Key'] == key:
                url = s3_client.generate_presigned_url('get_object', Params={'Bucket': custombucket, 'Key': item['Key']})

    except Exception as e:
        return str(e)
        
    finally:
        cursor.close()

    return render_template("GetEmpOutput.html", id=dEmpID, fname=dFirstName, lname=dLastName, interest=dPriSkill, location=dLocation, salary=dSalary, image_url=url)

@app.route("/getemp")
def getemp():
    return render_template('GetEmp.html')

@app.route("/upemp")
def upemp():
    return render_template('UpdateEmp.html')


@app.route("/delemp", methods=['POST'])
def delemp():
    # Get Employee
    emp_id = request.form['emp_id']
    # SELECT STATEMENT TO GET DATA FROM MYSQL
    select_sql = "SELECT COUNT(*) FROM employee WHERE emp_id = %s"
    selectCmd = "SELECT * FROM employee WHERE emp_id = %s"
    deleteCmd = "DELETE FROM employee WHERE emp_id = %s"
    cursor = db_conn.cursor()
    cursor1 = db_conn.cursor()
    key = "emp-id-" + str(emp_id) + "_image_file.png"
    s3 = boto3.client('s3')
    

    try:
        cursor.execute(select_sql, (emp_id,))
        result = cursor.fetchone()
        if result[0] == 0:
            return "Employee ID does not exist. Please enter a valid ID"
        
        cursor.execute(selectCmd, (emp_id,))
        cursor1.execute(deleteCmd, (emp_id,))
        # FETCH ONLY ONE ROWS OUTPUT
        row = cursor.fetchone()
        dempid = row[0]
        dFirstName = row[1]
        dLastName = row[2]
        emp_name = "" + dFirstName + " " + dLastName
        db_conn.commit()

        s3.delete_object(Bucket=custombucket, Key=key)
    except Exception as e:
        db_conn.rollback()
        return str(e)

    finally:
        cursor.close()
        cursor1.close()

    return render_template('DeleteEmpOutput.html', id=dempid, name=emp_name)

@app.route("/caltotalsalary", methods=['GET', 'POST'])
def caltotalsalary():
    min_emp_id = request.form['min_emp_id']
    max_emp_id = request.form['max_emp_id']
    select_sql = "SELECT COUNT(*) FROM employee WHERE emp_id BETWEEN %s AND %s"
    sqlCmd = "SELECT SUM(salary) AS totalsalary FROM employee WHERE emp_id BETWEEN %s AND %s"
    cursor = db_conn.cursor()

    if min_emp_id == "" or max_emp_id == "":
        return "Please enter an employee ID"

    if min_emp_id > max_emp_id:
        temp_id = min_emp_id
        min_emp_id = max_emp_id
        max_emp_id = temp_id

    try:
        cursor.execute(select_sql, (min_emp_id, max_emp_id))
        result = cursor.fetchone()

        expected_count = int(max_emp_id) - int(min_emp_id) + 1
        if result[0] != expected_count:
            return "Not all employee IDs exist within the specified range. Please select a valid range."

        # Getting Employee Data
        cursor.execute(sqlCmd, (min_emp_id, max_emp_id))
        row = cursor.fetchone()
        dmin_EmpID = min_emp_id
        dmax_EmpID = max_emp_id
        dtotal = row[0]

    except Exception as e:
        return str(e)
        
    finally:
        cursor.close()

    return render_template("TotalSalaryOutput.html", min_id=dmin_EmpID, max_id=dmax_EmpID, total=dtotal)

@app.route("/gotogetemp")
def gotogetemp():
    return render_template('GetEmp.html')

@app.route("/gotoupdateemp")
def gotoupdateemp():
    return render_template('UpdateEmp.html')

@app.route("/gotoaddemp")
def gotoaddemp():
    return render_template('AddEmp.html')

@app.route("/gotodeleteemp")
def gotodeleteemp():
    return render_template('DeleteEmp.html')

@app.route("/gotototalsalary")
def gotototalsalary():
    return render_template('TotalSalary.html')

@app.route("/zb")
def zb():
    return render_template('zb.html')

@app.route("/zm")
def zm():
    return render_template('zm.html')

@app.route("/ys")
def ys():
    return render_template('ys.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
