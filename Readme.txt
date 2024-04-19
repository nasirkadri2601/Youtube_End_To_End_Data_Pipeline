Quick overview of my project

Step 1: To extract data from youtube, first you need to create an API which will help you to extract data. You can create it from google developer console website.
Step 2: Go to youtube data api website. From there you can get all the sample codes which is required to extract data in all the programming languages (I have preferred python).
Step 3: To extract any data from youtube we need channelId of that channel. To extract channelId follow below steps:
--> Go to that channel and open any video.
--> Right click and open view page source.
--> Press ctrl + f and search channelid. From there you can find the channel id.
Step 4: From channel id I have extracted 3 sections of data i.e.
--> Channel Data
--> Videos Data
--> Comments Data
All these data comes in json format by default.
Step 5: For data extraction part I have used AWS Lambda which provides processing power for your python code to extract all the data and then save it to AWS S3 which is used to store all the raw data files.
Step 6: For data transformation I have used AWS Glue which is an ETL tool. Along with that we have used Apache Spark (PySpark) for processing all the data transformation within glue. Later the transformed data is stored in AWS S3. 
Step 7: For storing the transformed data we have used snowflake tool which is popular for its data warehousing functionality.
Step 8: We can integrate Power BI tool for creating interactive dashboards to understand the data.

Note: All the above process is automatic. I have used AWS cloud Watch to trigger Lambda function, within lambda function I have called Glue job which will do the transformation job, As soon as the transformed data is stored in S3, with the help of snowpipe the data is automatically loaded in tables within the snowflake datawarehouse.

Inside lambda funtion go to configurations --> Permission There you will see default role name is mentioned. Click that and attach permissions such as S3 full permission and AWS glue service role which will help to coordinate between lambda and glue services.

While creating pipeline connection from AWS to snowflake:
While creating storage integration, STORAGE_AWS_ROLE_ARN value should be taken from AWS IAM roles Create a new role and give S3 full access(inside IAM --> roles--> click on that role and select ARN value)
Also describe integration in snowflake. there you will see 2 values i.e. STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID. these value needs to be copy into AWS role which has S3 full access. go into specific role--> trust relationship. There replace AWS and External id value with these 2 values mentioned in the snowflake.
After creating pipes in snowflake describe the pipe and you will see notification channel. Copy that and create a event notification in S3 bucket properties.

If you need help then ping me on linkedin: https://www.linkedin.com/in/nasir-kadri-86573019b
