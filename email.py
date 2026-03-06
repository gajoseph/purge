import sys
import os 
import logging
import time
import getpass
from datetime import datetime,  date 

import socket


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart 


def send_email(subject:str, body:str, recipient:str, smtp_server_dict:dict):
    
    msg = MIMEMultipart()
    msg['From'] = smtp_server_dict["username"]
    msg['To'] = recipient 
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        with smtplib.SMTP(smtp_server_dict["hostname"], smtp_server_dict["port"]) as smtp_server:
            smtp_server.starttls()
            smtp_server.ehlo()
            smtp_server.login(smtp_server_dict["username"], smtp_server_dict["password"])
            smtp_server.send_message(msg)
            print("Successfully sent email")

    except smtplib.SMTPException as smtp_err:
        print(f"SMTP error occurred: {smtp_err}")

    except Exception as e:
        print(f"Error: unable to send email!!!\n\tError Description: {e}")

    


def build_send_email( __propdict, email_message_body:str):
   
    #now email if messages is not empty 
    # Get the hostname and IP
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)

    if email_message_body:
        email_recipient = __propdict['email.recipient']
        
        email_subject = f"Execution Alert: {__propdict['name']} pipeline failed on host: {hostname}({ip_address})"
        rerun_cmd = __propdict['rerun'].replace("\n", "<br>")
        email_message_body= f"""
            To manually restart the process login to the server, use the following command:-
            <br>
            <pre><code>{rerun_cmd}</code></pre>
            <hr>
            {email_message_body}
        """
        smtp_server_dict = {
        "hostname":  __propdict["smtp.hostname"],
        "port"    :  __propdict["smtp.port"],
        "username": __propdict["smtp.username"],
        "password": os.getenv("SMTPPASSWORD") # getting from .env file 
    }
        send_email(email_subject, email_message_body, email_recipient, smtp_server_dict) 

            
