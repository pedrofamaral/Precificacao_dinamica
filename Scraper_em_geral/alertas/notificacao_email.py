"""
Envia e-mails via Gmail SMTP (porta SSL 465).
Use APP PASSWORD – não a senha “normal”.
"""
import os, smtplib, ssl
from email.message import EmailMessage

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT   = 465

SENDER      = os.getenv("amaralpedro358@gmail.com")
PASSWORD    = os.getenv("yocj fmga qfag jxmx")
RECIPIENTS = ["amaralpedro358@gmail.com"]



def send_email(subject: str, body: str):
    if not (SENDER and PASSWORD):
        raise RuntimeError("Variáveis GMAIL_USER/GMAIL_APP_PASSWORD não definidas.")

    msg = EmailMessage()
    msg["From"]    = SENDER
    msg["To"]      = ", ".join(RECIPIENTS)
    msg["Subject"] = subject
    msg.set_content(body)

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=ctx) as smtp:
        smtp.login(SENDER, PASSWORD)
        smtp.send_message(msg)
