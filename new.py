import re
import datetime
import mysql.connector
import pymongo

fh = open(r'C:\Users\Sangeetha\Desktop\mbox.txt','r')
data=fh.read()
output = []
#reading data as txt file
with open(r'C:\Users\Sangeetha\Desktop\mbox.txt', 'r') as file:
    lines = file.readlines()
#using regex to find mail and date
for line in lines:
    email = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', line)

    date = re.findall(r'[\w:]+\s+\d+\s\d+:\d+:\d+\s\d+',line)
    d1 = [datetime.datetime.strptime(x, '%b  %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S') for x in date]

    if len(email) != 0:
        output.append({"email":email[0],"date": "" if len(d1) == 0 else d1[0]})

#converting python file to mongodb:

client=pymongo.MongoClient("mongodb://localhost:27017")

db=client["user_proj"]
db.user_history.insert_many(output)
#connecting mongodb with mysql:

connection=mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="proj2"
)

cursor = connection.cursor()
#various quries over table:
x=int(input("enter question number [1:10]:"))
#question:1 extracting all unique email ids:
if x==1:
    cursor.execute("select distinct(email) as emails from user_data1 group by email")
    print(cursor.fetchall())

# #question:2 printing email and count of mails for unique email id:
elif x==2:
    cursor.execute("select email,count(email) from user_data1 group by email order by 2 desc")
    print(cursor.fetchall())

#question:3 printing emails and date of email received
elif x==3:
    cursor.execute("SELECT email,date_format(email_date,'%Y-%m-%d %H:%i:%S') as new_date FROM user_data1")
    print(cursor.fetchall())

#question:4 printing count of email on each day
elif x==4:
    cursor.execute("select date_format(email_date,'%Y-%m-%d %H:%i:%S') as new_date,count(email) as count_of_mail from user_data1 group by 1")
    print(cursor.fetchall())

#qn 5: printing each domain and its count of mails:
elif x==5:
    cursor.execute("""SELECT
                  SUBSTRING_INDEX( email, '@', -1 ) as domain ,
                  count(*) as count_domain
                  FROM user_data1 GROUP BY SUBSTRING_INDEX( email, '@', -1 ) ORDER BY 2 DESC""")
    print(cursor.fetchall())

#qn 6:printing first and last mail received from each mail:
elif x==6:
    cursor.execute("""select
                    email,
                    max(email_date) as last_mail,
                    min(email_date) as first_mail from user_data1 group by 1""")
    print(cursor.fetchall())

#qn:7 printing which month mails where received mostly:
elif x==7:
    cursor.execute("""with cte as (select date_format(email_date,'%Y-%m-%d %H:%i:%S') as new_date from user_data1)
                    select
                    extract(month from new_date) as month,
                    count(*) as mails_received
                    from cte
                    group by 1""")
    print(cursor.fetchall())

#qn 8: printing mails where from in server-log files:
elif x==8:
    cursor.execute("select email,email_date from user_data1 where email_date != '' ")
    print(cursor.fetchall())

#qn: 9: printing maximum mails received from which mail:
elif x==9:
    cursor.execute("""with cte as (select email,count(email) as count1 from user_data1 
                    group by email 
                    order by 2 desc)"
                    select email,count1 from cte limit 1""")
    print(cursor.fetchall())


#qn 10: spliting email as two parts with @:
elif x==10:
    cursor.execute("""select email,substring_index(email,'@',1) as user_name,
                    substring_index(email,'@',-1) as domain_name from user_data1 
                    group by 1""")
    print(cursor.fetchall())