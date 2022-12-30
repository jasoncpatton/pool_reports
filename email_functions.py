import smtplib
import dns.resolver
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

def send_email(from_addr, to_addrs, subject="", replyto_addr=None, cc_addrs=[], bcc_addrs=[], attachments=[], html="", text=""):
    if len(to_addrs) == 0:
        logging.error("No recipients in the To: field, not sending email")
        return

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if len(cc_addrs) > 0:
        msg["Cc"] = ", ".join(cc_addrs)
    if len(bcc_addrs) > 0:
        msg["Bcc"] = ", ".join(bcc_addrs)
    if replyto_addr is not None:
        msg["Reply-To"] = replyto_addr
    msg["Subject"] = subject

    if text:
        msg.attach(MIMEText(text, "plain"))

    if html:
        msg.attach(MIMEText(html, "html"))
    
    for attachment in attachments:
        path = Path(attachment)
        part = MIMEBase("application", "octet-stream")
        with path.open("rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename = path.name)
        msg.attach(part)

    for recipient in to_addrs + cc_addrs + bcc_addrs:
        domain = recipient.split("@")[1]
        sent = False
        result = None
        for mx in dns.resolver.query(domain, "MX"):
            mailserver = str(mx).split()[1][:-1]
            try:
                smtp = smtplib.SMTP(mailserver)
                result = smtp.sendmail(from_addr, recipient, msg.as_string())
                smtp.quit
            except Exception:
                if result is not None:
                    print(f"Got result: {result}")
                print(f"Could not send to {recipient} using {mailserver}")
            else:
                sent = True
            if sent:
                break
