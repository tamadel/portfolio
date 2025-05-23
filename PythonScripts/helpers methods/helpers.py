

#----------------------------------------------------------------------------------------------------
# stopExecution(): Verabeitung abbrechen
#----------------------------------------------------------------------------------------------------
def stopExecution(
        script,
        status_msg, 
        logger = None, 
        error_flag = False, 
        sendEmailFlag = False,
        emailSettings = {},
        connection = [],
        mount_dir = None,
    ):
    import shlex
    from subprocess import check_output, STDOUT, call
    from datetime import datetime
    
    if connection:
        if isinstance(connection, list):
            for conn in connection:
                if conn:
                    conn.close()
        else:
            connection.close()

    if mount_dir:
        try:
            cmd = f"sudo umount {mount_dir}"
            cmd = shlex.split(cmd)
            check_output(cmd, stderr=STDOUT).decode()
            if logger:
                logger.debug( f"stopExecution() - Verbindung zu '{mount_dir}' getrennt" )

        except Exception as ex:
            if logger:
                logger.error( f"Fehler beim Trennen der Verbindung zu '{mount_dir}': {ex}" )

    if logger:
        if error_flag:
            logger.error( f"{script} - {status_msg}" )
        else:
            logger.info( f"{script} - {status_msg}" )

    if error_flag:
        if emailSettings:
            subject = f"ERROR - {emailSettings['subject']}"
    else:
        if emailSettings:
            subject = f"INFO - {emailSettings['subject']}"


    if sendEmailFlag and emailSettings:
        sendSESEmail(
           aws_region = 'eu-central-1',
           sender = emailSettings["email_from"],
           recipients = emailSettings["support"],
           subject = subject,
           body_txt = status_msg
        )

    else:
        print(status_msg)

#----------------------------------------------------------------------------------------------------
# initLogger(): Logger initialisieren
#----------------------------------------------------------------------------------------------------
def initLogger(name, stdout=False, log_dir=None, log_file=None, log_level="DEBUG"):
    import logging
    import sys
    from datetime import datetime

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if log_level.upper() == "CRITICAL":
        l_level = logging.CRITICAL
    elif log_level.upper() == "ERROR":
        l_level = logging.ERROR 
    elif log_level.upper() == "WARNING":
        l_level = logging.WARNING 
    elif log_level.upper() == "INFO":
        l_level = logging.INFO 
    else:
        l_level = logging.DEBUG 

    if not(log_file) and log_dir:
        log_file = log_dir + "/logs_" + datetime.strftime(datetime.now(),"%Y%m%d_%H%M%S") + ".log"

    if log_dir:
        checkAndMakeDir(log_dir)

    if stdout:
        handler = logging.StreamHandler(sys.stdout) 
    else:
        handler = logging.FileHandler(log_file)

    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.addHandler(handler)

    return logger

#----------------------------------------------------------------------------------------------------
# checkAndMakeDir(): Verzeichnis überprüfen und erstellen, falls noch nicht vorhanden
#----------------------------------------------------------------------------------------------------
def checkAndMakeDir(dir):
    import os

    # Verzeichnis erstellen, falls nicht vorhanden
    if not(os.path.exists(dir)):
        try:
            os.makedirs(dir)
            return 0, "OK - Directory '%(dir)s' created." %locals()
        except Exception as err:
            error_msg = str(err)
            return 1, "ERROR - failed to create directory '%(dir)s' (%(error_msg)s)" %locals()

    return 0, "CHECKED - Directory '%(dir)s' exists." %locals()

#----------------------------------------------------------------------------------------------------
# sendEmail(): Email-Versand über Google Account
#----------------------------------------------------------------------------------------------------
def sendEmail( email_from, recipients, cc=[], subject="", body="", att="", aws_region="eu-central-1" ):
    import boto3
    from email.mime.multipart import MIMEMultipart 
    from email.mime.text import MIMEText 
    from email.mime.base import MIMEBase 
    from email import encoders 
  
     
    # instance of MIMEMultipart 
    msg = MIMEMultipart() 

    # Absender
    msg['From'] = email_from 

    # Empfänger
    if isinstance(recipients, list):
        msg['To'] = ", ".join(recipients)
    else:
        msg['To'] = recipients
        recipients = [ recipients ]

    # Betreff
    msg['Subject'] = subject

    # Email-Text
    msg.attach( MIMEText(body, "plain") )

    # Attachement
    if att:
        try:
            filename = att.split("/")[-1]
            attachment = open(att, "rb")

            # Instanz auf MIMEBase
            p = MIMEBase("application", "octet-stream")
            p.set_payload( (attachment).read() )

            # Encode
            encoders.encode_base64(p)

            # Header setzen
            p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(p)
        except Exception as e:
            msg = str(e)
            return 1, "Error while creating attachement (%(msg)s)." %locals()

    text = msg.as_string()

    # Mail versenden
    try:
        ses_client = boto3.client('ses', region_name=aws_region)
        ses_client.send_raw_email(
            Source=email_from,
            Destinations=recipients,
            RawMessage={
                'Data': text
            }
        )
    except Exception as e:
        msg = str(e)
        return 1, "Error while sending email (%(msg)s)." %locals()

    return 0, "OK"

#----------------------------------------------------------------------------------------------------
# sendSESEmail(): Email-Versand über AWS Simple Email Service (SES)
#----------------------------------------------------------------------------------------------------
def sendSESEmail( aws_region, sender, recipients, subject, body_txt="", body_html="" ):
    import boto3
    from botocore.exceptions import ClientError

    # Empfänger
    if isinstance(recipients, list):
        recipients  = recipients
    else:
        recipients = [recipients]

    if not(body_html):
        body_html = f"""
            <html>
                <head></head>
                <body>
                    <p>{body_txt}</p>
                </body>
            </html>
        """

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=aws_region)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': recipients,
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': body_html,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': body_txt,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': subject,
                },
            },
            Source=sender,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        raise Exception( e.response['Error']['Message'] )
    
    return 0, f"Email versendet! Message ID: {response['MessageId']}"

#----------------------------------------------------------------------------------------------------
# getSecrets(): Secrets aus AWS Secrets Manager extrahieren
#----------------------------------------------------------------------------------------------------
def getSecrets( aws_region, secret_key_list, credentials={} ):
    import boto3
    import base64
    import json
    
    # Secrets Manager Client erstellen
    try:
        session = boto3.session.Session()
        if credentials:
            client = session.client(
                service_name='secretsmanager',
                region_name = aws_region,
                aws_access_key_id = credentials['AccessKeyId'],
                aws_secret_access_key = credentials['SecretAccessKey'],
                aws_session_token = credentials['SessionToken']
            )

        else:
            client = session.client(
                service_name='secretsmanager',
                region_name = aws_region
            )
    
    except Exception as ex:
        raise Exception(ex)

    secrets = {}
    for secret_key in secret_key_list:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId = secret_key
            )
            if 'SecretString' in get_secret_value_response:
                try:
                    secret = json.loads( get_secret_value_response['SecretString'] ) 
                except Exception as ex:
                    try:
                        secret = get_secret_value_response['SecretString']
                    except Exception as ex:
                        raise Exception( ex )
            else:
                secret = json.loads( base64.b64decode(get_secret_value_response['SecretBinary']) )

            secrets[ secret_key ] = secret
        
        except Exception as ex:
            raise Exception(ex)

    return secrets

#----------------------------------------------------------------------------------------------------
# getWebGISSecrets(): Secrets aus AWS Secrets Manager in der WebGIS-Umgebung extrahieren
#----------------------------------------------------------------------------------------------------
def getWebGISSecrets( aws_region, role_arn, secret_key_list, credentials={} ):
    import boto3
    import base64
    import json
    
    # STS-Client erstellen
    try:
        sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn = role_arn,
            RoleSessionName = "AssumeRoleSession1"
        )
    
    except Exception as ex:
        raise Exception(ex)
    
    # Session-Client erstellen
    try:
        credentials = assumed_role_object['Credentials']
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name = aws_region,
            aws_access_key_id = credentials['AccessKeyId'],
            aws_secret_access_key = credentials['SecretAccessKey'],
            aws_session_token = credentials['SessionToken']
        )
    
    except Exception as ex:
        raise Exception(ex)

    secrets = {}
    for secret_key in secret_key_list:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId = secret_key
            )
            if 'SecretString' in get_secret_value_response:
                try:
                    secret = json.loads( get_secret_value_response['SecretString'] ) 
                except Exception as ex:
                    try:
                        secret = get_secret_value_response['SecretString']
                    except Exception as ex:
                        raise Exception( ex )
            else:
                secret = json.loads( base64.b64decode(get_secret_value_response['SecretBinary']) )

            secrets[ secret_key ] = secret
        
        except Exception as ex:
            raise Exception(ex)

    return secrets

#----------------------------------------------------------------------------------------------------
# exportDataFrameToS3(): Export eine Panda DataFrames nach S3
#----------------------------------------------------------------------------------------------------
def exportDataFrameToS3( aws_region, bucket, df, output_file, role_arn=None ):
    import boto3
    from io import StringIO
    
    # STS-Client erstellen
    if role_arn:
        try:
            sts_client = boto3.client('sts')
            assumed_role_object = sts_client.assume_role(
                RoleArn = role_arn,
                RoleSessionName = "AssumeRoleSession1"
            )
        
        except Exception as ex:
            raise Exception(ex)
        
        # Session-Client erstellen
        try:
            credentials = assumed_role_object['Credentials']
            session = boto3.session.Session()
            s3_client = session.client(
                service_name='s3',
                region_name = aws_region,
                aws_access_key_id = credentials['AccessKeyId'],
                aws_secret_access_key = credentials['SecretAccessKey'],
                aws_session_token = credentials['SessionToken']
            )
    
        except Exception as ex:
            raise Exception(ex)
            
    else:
        s3_client = boto3.client('s3')
    
    # Dataframe nach S3 exportieren
    try:
        csv_buffer = StringIO()
        df.to_csv(
            csv_buffer,
            sep = '|',
            index = False,
        )
        s3_client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket,
            Key=output_file
        )             
    
    except Exception as ex:
        raise Exception(ex)

#----------------------------------------------------------------------------------------------------
# getFoldersInS3Bucket(): get list of folders in a S3-Bucket
#----------------------------------------------------------------------------------------------------
def getFoldersInS3Bucket( bucket, prefix="", aws_region="eu-central-1", credentials=None ):
    import boto3
    import os
    import inspect
    
    func_name = inspect.currentframe().f_code.co_name
    
    try:
            
        if credentials:
            client = boto3.client(
                's3',
                aws_access_key_id = credentials['AccessKeyId'],
                aws_secret_access_key = credentials['SecretAccessKey'],
                aws_session_token = credentials['SessionToken']
            )
            
        else:
            client = boto3.client('s3')
            
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )
            
        folder_list = set()
        for r in response['Contents']:
            file_name = r['Key'].replace(prefix+"/","")
            if len( file_name.split("/") ) > 1:
                folder_list.add( file_name.split("/")[-2] )
                
        return folder_list
    
    except Exception as ex:
        error_msg = f"{func_name}() - Fehler beim Erstellen der Liste der Verzeichnisse in 's3://{bucket}/{prefix}': {ex}"
        raise Exception(error_msg)

#----------------------------------------------------------------------------------------------------
# getFilesInS3Bucket(): get list of files in a S3-Bucket
#----------------------------------------------------------------------------------------------------
def getFilesInS3Bucket( bucket, prefix="", file_suffix="" ):
    import boto3
    import os
    import inspect
    
    func_name = inspect.currentframe().f_code.co_name
    
    try:
            
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )
            
        file_list = []
        for r in response['Contents']:
            file_name = r['Key']
            if file_name:
                if file_suffix:
                    if file_name.lower().endswith(file_suffix.lower()):
                        file_list.append( file_name )
                else:
                    file_list.append( file_name )
                
        return file_list
    
    except Exception as ex:
        error_msg = f"{func_name}() - Fehler beim Erstellen der Liste der Files in 's3://{bucket}/{prefix}': {ex}"
        raise Exception(error_msg)

#----------------------------------------------------------------------------------------------------
# deleteFilesInS3Folder(): delete existig files in a S3-Folder
#----------------------------------------------------------------------------------------------------
def deleteFilesInS3Folder( bucket, prefix="", file_suffix="", credentials=None ):
    import inspect
    import boto3
     
    func_name = inspect.currentframe().f_code.co_name
    
    try:
        if credentials:
            client = boto3.client(
                's3',
                aws_access_key_id = credentials['AccessKeyId'],
                aws_secret_access_key = credentials['SecretAccessKey'],
                aws_session_token = credentials['SessionToken']
            )
            
        else:
            client = boto3.client('s3')
            
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )
        if response['KeyCount'] > 0:
            for r in response['Contents']:
                file_name = r['Key']
                if file_name and file_name != prefix+"/":
                    if file_suffix:
                        if file_name.lower().endswith(file_suffix.lower()):
                            client.delete_object(
                                Bucket=bucket, 
                                Key=file_name
                            )
                    else:
                        client.delete_object(
                            Bucket=bucket, 
                            Key=file_name
                        )
                
        return 0
    
    except Exception as ex:
        error_msg = f"{func_name}() - Fehler beim Löschen der Files in 's3://{bucket}/{prefix}': {ex}"
        raise Exception(error_msg)
