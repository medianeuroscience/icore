import smtplib, ssl

smtp_server = "smtp.gmail.com"
port = 587
sender_email = "medianeuroscience.sb@gmail.com"
receiver_email = "mim290@nyu.edu"
password = input("Type your password and press enter: ")
message = """\
Subject: Hi there

This message is sent from Python.
"""

#create secure ssl context
context = ssl.create_default_context()

with smtplib.SMTP(smtp_server, port) as server:
    server.ehlo()
    server.starttls(context=context)
    server.ehlo()
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)
