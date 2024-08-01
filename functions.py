import re
import sys
import time
import pickle
import smtplib
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from urllib.error import HTTPError
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from pathlib import Path

import htcondor
from dns.resolver import query as dns_query


TOPOLOGY_PROJECT_DATA_URL = "https://topology.opensciencegrid.org/miscproject/xml"
TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"

OSPOOL_APS = {
    "ap20.uc.osg-htc.org",
    "ap2007.chtc.wisc.edu",
    "ap21.uc.osg-htc.org",
    "ap22.uc.osg-htc.org",
    "ap23.uc.osg-htc.org",
    "ap40.uw.osg-htc.org",
    "ap41.uw.osg-htc.org",
    "ap42.uw.osg-htc.org",
    "ap7.chtc.wisc.edu",
    "ap7.chtc.wisc.edu@ap2007.chtc.wisc.edu",
    "ce1.opensciencegrid.org",
    "comses.sol.rc.asu.edu",
    "condor.scigap.org",
    "descmp3.cosmology.illinois.edu",
    "gremlin.phys.uconn.edu",
    "htcss-dev-ap.ospool.opensciencegrid.org",
    "huxley-osgsub-001.sdmz.amnh.org",
    "lambda06.rowan.edu",
    "login-el7.xenon.ci-connect.net",
    "login-test.osgconnect.net",
    "login.ci-connect.uchicago.edu",
    "login.collab.ci-connect.net",
    "login.duke.ci-connect.net",
    "login.snowmass21.io",
    "login.veritas.ci-connect.net",
    "login04.osgconnect.net",
    "login05.osgconnect.net",
    "mendel-osgsub-001.sdmz.amnh.org",
    "nsgosg.sdsc.edu",
    "os-ce1.opensciencegrid.org",
    "os-ce1.osgdev.chtc.io",
    "osg-prp-submit.nautilus.optiputer.net",
    "osg-vo.isi.edu",
    "ospool-eht.chtc.wisc.edu",
    "xd-submit0000.chtc.wisc.edu",
    "testbed",
}
OSPOOL_COLLECTORS = {
    "cm-1.ospool.osg-htc.org",
    "cm-2.ospool.osg-htc.org",
    "flock.opensciencegrid.org",
}
NON_OSPOOL_RESOURCES = {
    "SURFsara",
    "NIKHEF-ELPROD",
    "INFN-T1",
    "IN2P3-CC",
    "UIUC-ICC-SPT",
    "TACC-Frontera-CE2",
}


def get_topology_project_data(cache_file=Path("./topology_project_data.pickle")) -> dict:
    if cache_file.exists() and cache_file.stat().st_mtime > time.time() - 23*3600:
        try:
            projects_map = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
        else:
            return projects_map
    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_PROJECT_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries:
                raise
        else:
            break
    projects = xmltree.getroot()
    projects_map = {
        "Unknown": {
            "name": "Unknown",
            "pi": "Unknown",
            "pi_institution": "Unknown",
            "field_of_science": "Unknown",
        }
    }

    for project in projects:
        project_map = {}
        project_map["name"] = project.find("Name").text
        project_map["pi"] = project.find("PIName").text
        project_map["pi_institution"] = project.find("Organization").text
        project_map["field_of_science"] = project.find("FieldOfScience").text
        project_map["id"] = project.find("ID").text
        project_map["pi_institution_id"] = project.find("InstitutionID").text
        project_map["field_of_science_id"] = project.find("FieldOfScienceID").text
        projects_map[project_map["name"].lower()] = project_map.copy()

    pickle.dump(projects_map, cache_file.open("wb"))
    return projects_map


def get_topology_resource_data(cache_file=Path("./topology_resource_data.pickle")) -> dict:
    if cache_file.exists() and cache_file.stat().st_mtime > time.time() - 23*3600:
        try:
            resources_map = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
        else:
            return resources_map
    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_RESOURCE_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries:
                raise
        else:
            break
    resource_groups = xmltree.getroot()
    resources_map = {
        "Unknown": {
            "name": "Unknown",
            "institution": "Unknown",
        }
    }

    for resource_group in resource_groups:
        resource_institution = resource_group.find("Facility").find("Name").text
        resource_institution_id = resource_group.find("Facility").find("ID").text

        resources = resource_group.find("Resources")
        for resource in resources:
            resource_map = {}
            resource_map["institution"] = resource_institution
            resource_map["institution_id"] = resource_institution_id
            resource_map["name"] = resource.find("Name").text
            resource_map["id"] = resource.find("ID").text
            resources_map[resource_map["name"].lower()] = resource_map.copy()

    pickle.dump(resources_map, cache_file.open("wb"))
    return resources_map


def get_ospool_aps() -> set:
    current_ospool_aps = set()
    for collector_host in OSPOOL_COLLECTORS:
        try:
            collector = htcondor.Collector(collector_host)
            aps = collector.query(htcondor.AdTypes.Schedd, projection=["Machine", "CollectorHost"])
        except Exception:
            continue
        for ap in aps:
            if set(re.split(r"[\s,]+", ap["CollectorHost"])) & OSPOOL_COLLECTORS:
                current_ospool_aps.add(ap["Machine"])
    return current_ospool_aps | OSPOOL_APS


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
            print(f"Could not connect to {smtp_server}", file=sys.stderr)
            continue

        try:
            result = smtp.sendmail(msg["From"], recipient, msg.as_string())
            if len(result) > 0:
                print(f"Could not send email to {recipient} using {smtp_server}:\n{result}", file=sys.stderr)
            else:
                sent = True
        except Exception:
            print(f"Could not send to {recipient} using {smtp_server}", file=sys.stderr)
            print(err, file=sys.stderr)
        finally:
            try:
                smtp.quit()
            except smtplib.SMTPServerDisconnected:
                pass
        if sent:
            break

        sleeptime = int(min(30 * 1.5**tries, 600))
        print(f"Sleeping for {sleeptime} seconds before retrying servers", file=sys.stderr)
        time.sleep(sleeptime)
        tries += 1

    else:
        print(f"Failed to send email after {tries} loops", file=sys.stderr)

    return sent


def send_email(
        subject,
        from_addr,
        to_addrs=[],
        html="",
        cc_addrs=[],
        bcc_addrs=[],
        reply_to_addr=None,
        attachments=[],
        smtp_server=None,
        smtp_username=None,
        smtp_password_file=None,
        **kwargs):
    if len(to_addrs) == 0:
        print("ERROR: No recipients in the To: field, not sending email", file=sys.stderr)
        return

    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if len(cc_addrs) > 0:
        msg["Cc"] = ", ".join(cc_addrs)
    if len(bcc_addrs) > 0:
        msg["Bcc"] = ", ".join(bcc_addrs)
    if reply_to_addr is not None:
        msg["Reply-To"] = reply_to_addr
    msg["Subject"] = subject
    msg["Message-ID"] = f"<{subject}-{time.time()}-{from_addr}>".replace(" ", "-").casefold()
    msg["Date"] = formatdate(localtime=True)

    msg.attach(MIMEText(html, "html"))

    for fname in attachments:
        fpath = Path(fname)
        part = MIMEBase("application", "octet-stream")
        with fpath.open("rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename = fpath.name)
        msg.attach(part)

    if smtp_server is not None:
        recipient = list(set(to_addrs + cc_addrs + bcc_addrs))
        smtp_password = None
        if smtp_password_file is not None:
            smtp_password = smtp_password_file.open("r").read().strip()
        _smtp_mail(msg, recipient, smtp_server, smtp_username, smtp_password)

    else:
        for recipient in set(to_addrs + cc_addrs + bcc_addrs):
            domain = recipient.split("@")[1]
            sent = False
            for mxi, mx in enumerate(dns_query(domain, "MX")):
                smtp_server = str(mx).split()[1][:-1]

                try:
                    sent = _smtp_mail(msg, recipient, smtp_server)
                except Exception:
                    continue
                if sent:
                    break

                sleeptime = int(min(30 * 1.5**mxi, 600))
                print(f"Sleeping for {sleeptime} seconds before trying next server", file=sys.stderr)
                time.sleep(sleeptime)

            else:
                print("Failed to send email after trying all servers", file=sys.stderr)
