import smtplib
import dns.resolver
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path


def _smtp_mail(msg, recipient, smtp_server=None, smtp_username=None, smtp_password=None):
    sent = False
    result = None
    tries = 0
    sleeptime = 0
    while tries < 3 and sleeptime < 600:
        try:
            if smtp_username is None:
                smtp = smtplib.SMTP(smtp_server)
            else:
                smtp = smtplib.SMTP_SSL(smtp_server)
                smtp.login(smtp_username, smtp_password)
        except Exception:
            print(f"Could not connect to {smtp_server}")
            continue

        try:
            result = smtp.sendmail(msg["From"], recipient, msg.as_string())
            if len(result) > 0:
                print(f"Could not send email to {recipient} using {smtp_server}:\n{result}")
            else:
                sent = True
        except Exception:
            print(f"Could not send to {recipient} using {smtp_server}")
        finally:
            try:
                smtp.quit()
            except smtplib.SMTPServerDisconnected:
                pass
        if sent:
            break

        sleeptime = int(min(30 * 1.5**tries, 600))
        print(f"Sleeping for {sleeptime} seconds before retrying servers")
        time.sleep(sleeptime)
        tries += 1

    else:
        print(f"Failed to send email after {tries} loops")

    return sent


def send_email(from_addr, to_addrs, subject="", replyto_addr=None, cc_addrs=[], bcc_addrs=[], attachments=[], html="", text="",
                smtp_server=None, smtp_username=None, smtp_password_file=None):
    if len(to_addrs) == 0:
        print("No recipients in the To: field, not sending email")
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

    if smtp_server is not None:
        recipient = list(set(to_addrs + cc_addrs + bcc_addrs))
        smtp_password = None
        if smtp_password_file is not None:
            smtp_password = smtp_password_file.open("r").read().strip()
        _smtp_mail(msg, recipient, smtp_server, smtp_username, smtp_password)

    else:
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
