from datetime import datetime, timedelta
from collections import OrderedDict
from string import ascii_uppercase
from pathlib import Path
import requests
import argparse
import pickle
import elasticsearch
import xlsxwriter
import sys

from pull_topology import get_mappings
from get_ospool_aps import get_ospool_aps, OSPOOL_COLLECTORS
from email_functions import send_email


NOW = datetime.now()

TO = ["ospool-reports@path-cc.io"]
TIMEOUT = 180
DAYS = 365

OSPOOL_DAILY_TOTALS_INDEX = "daily_totals"
OSPOOL_DAILY_TOTALS_QUERY_ID = "OSG-schedd-job-history"
OSPOOL_DAILY_REPORT_PERIOD = "daily"

OSPOOL_RAW_DATA_INDEX = "osg_schedd_write"

OSPOOL_APS = get_ospool_aps()
OSPOOL_NON_FAIRSHARE_RESOURCES = {
    "SURFsara",
    "NIKHEF-ELPROD",
    "INFN-T1",
    "IN2P3-CC",
    "UIUC-ICC-SPT",
    "TACC-Frontera-CE2",
}

RESOURCE_TO_INSTITUTION = get_mappings()["facility"]
PROJECT_TO_INSTITUTION = {k.casefold(): v for k, v in requests.get("https://topology.opensciencegrid.org/miscproject/json").json().items()}


def get_daily_totals_query(start_dt, end_dt):
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")
    query = {
        "index": OSPOOL_DAILY_TOTALS_INDEX,
        "size": 0,
        "track_scores": False,
        "aggs": {
            "total_jobs": {
                "sum": {
                    "field": "num_uniq_job_ids",
                    "missing": 0,
                }
            },
            "core_hours": {
                "sum": {
                    "field": "all_cpu_hours",
                    "missing": 0,
                }
            },
            "files_transferred": {
                "sum": {
                    "field": "total_files_xferd",
                    "missing": 0,
                }
            },
        },
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "date": {
                            "gte": start_date,
                            "lt": end_date,
                        }
                    }},
                    {"term": {
                        "query": {"value": OSPOOL_DAILY_TOTALS_QUERY_ID}
                    }},
                    {"term": {
                        "report_period": {"value": OSPOOL_DAILY_REPORT_PERIOD}
                    }},
                ]
            }
        }
    }
    return query


def get_raw_data_query(start_dt, end_dt):
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    query = {
        "index": OSPOOL_RAW_DATA_INDEX,
        "size": 0,
        "track_scores": False,
        "runtime_mappings": {
            "ResourceName": {
                "type": "keyword",
                "script": {
                    "language": "painless",
                    "source": """
                        String res;
                        if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
                            res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
                        } else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
                            res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
                        } else {
                            res = "UNKNOWN";
                        }
                        emit(res);
                        """,
                }
            }
        },
        "aggs": {
            "users": {
                "terms": {
                    "field": "User.keyword",
                    "missing": "UNKNOWN",
                    "size": 1024,
                }
            },
            "resources": {
                "terms": {
                    "field": "ResourceName",
                    "missing": "UNKNOWN",
                    "size": 1024,
                },
            },
            "projects": {
                "terms": {
                    "field": "ProjectName.keyword",
                    "missing": "UNKNOWN",
                    "size": 1024,
                }
            },
        },
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "RecordTime": {
                            "gte": start_ts,
                            "lt": end_ts,
                        }
                    }},
                    {"term": {
                        "JobUniverse": 5,
                    }},
                ],
                "minimum_should_match": 1,
                "should" : [
                    {"bool": {
                        "filter": [
                            {"terms": {
                                "ScheddName.keyword": list(OSPOOL_APS)
                            }},
                        ],
                        "must_not": [
                            {"exists": {
                                "field": "LastRemotePool",
                            }},
                        ],
                    }},
                    {"terms": {
                        "LastRemotePool.keyword": list(OSPOOL_COLLECTORS)
                    }},
                ],
                "must_not": [
                    {"terms": {
                        "ResourceName": list(OSPOOL_NON_FAIRSHARE_RESOURCES)
                    }},
                ],
            }
        }
    }
    return query


def print_error(d, depth=0):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif k == "root_cause":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_error(v, depth=depth+1)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}")
        else:
            print(f"{pre}{k}:\t{v}")


def do_query(client, query):
    try:
        result = client.search(index=query.pop("index"), body=query, request_timeout=TIMEOUT)
    except Exception as e:
        print_error(e.info)
        raise
    return result


def get_timestamps():
    dates = OrderedDict()

    def prev_month(dt, end_day):
        (year, month, day,) = dt.timetuple()[0:3]
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        if month in {9, 4, 6, 11} and day > 30:
            day = 30
        elif month == 2 and day > 28:
            day = 28
        else:
            day = end_day
        return datetime(year, month, day)

    dt_end = datetime(*NOW.timetuple()[0:3])
    dt_end_day = dt_end.day
    dt_stop = dt_end - timedelta(days=DAYS)
    while dt_end > dt_stop:
        dt_start = prev_month(dt_end, dt_end_day)
        datestr = f"{dt_start.strftime('%Y-%m-%d')}"
        dates[datestr] = (dt_start, dt_end,)
        dt_end = dt_start
    
    return dates


def get_monthly_docs(client):

    docs = OrderedDict()
    timestamps = get_timestamps()

    bucket_names = ["users", "projects", "institutions_contrib", "institutions_benefit"]
    sum_names = ["total_jobs", "core_hours", "files_transferred"]

    docs["TOTAL"] = {sum_name: 0. for sum_name in sum_names}
    docs["TOTAL"]["date"] = "TOTAL"
    total_datasets = {bucket_name: set() for bucket_name in bucket_names}

    unknown_resources = set()
    unknown_projects = set()

    for datestr, (start_dt, end_dt,) in timestamps.items():
        docs[datestr] = {"date": datestr}

        query = get_daily_totals_query(start_dt, end_dt)
        results = do_query(client, query)

        for sum_name in sum_names:
            value = results.get("aggregations", {}).get(sum_name, {}).get("value", 0.)
            docs[datestr][sum_name] = value
            docs["TOTAL"][sum_name] += value

        raw_datasets = {bucket_name: set() for bucket_name in bucket_names}
        query = get_raw_data_query(start_dt, end_dt)
        results = do_query(client, query)

        for bucket_name in bucket_names:
            if bucket_name == "institutions_benefit":
                continue
            if bucket_name == "institutions_contrib":
                buckets = results.get("aggregations", {}).get("resources", {}).get("buckets", {})
            else:
                buckets = results.get("aggregations", {}).get(bucket_name, {}).get("buckets", {})
            for bucket in buckets:
                if bucket_name == "institutions_contrib":
                    value = RESOURCE_TO_INSTITUTION.get(bucket["key"], "UNKNOWN")
                    if (value == "UNKNOWN") and (bucket["key"] != "UNKNOWN") and (bucket["key"] not in unknown_resources):
                        print(f"Unknown resource name: {bucket['key']}")
                        unknown_resources.add(bucket["key"])
                elif bucket_name == "users" and '@' in bucket["key"]:
                    user, domain = bucket["key"].split("@")
                    value = user.casefold()
                elif bucket_name == "projects":
                    value = bucket["key"].casefold()
                    project_info = PROJECT_TO_INSTITUTION.get(value)
                    if project_info is not None:
                        raw_datasets["institutions_benefit"].add(project_info["Organization"].casefold())
                        total_datasets["institutions_benefit"].add(project_info["Organization"].casefold())
                    elif value not in unknown_projects:
                        unknown_projects.add(value)
                        print(f"Project missing from institution map: {bucket['key']}")
                else:
                    value = bucket["key"].casefold()
                if value.casefold() in {"", "unknown"}:
                    continue
                raw_datasets[bucket_name].add(value)
                total_datasets[bucket_name].add(value)
            docs[datestr][bucket_name] = len(raw_datasets[bucket_name])
            if bucket_name == "projects":
                docs[datestr]["institutions_benefit"] = len(raw_datasets["institutions_benefit"])

    for bucket_name in bucket_names:
        print(f"{bucket_name}:")
        for i, value in enumerate(sorted(list(total_datasets[bucket_name]))):
            print(f"\t{i+1}. {value}")
        docs["TOTAL"][bucket_name] = len(total_datasets[bucket_name])

    return docs


def write_xlsx_html(docs, xlsx_file):
    headers = OrderedDict([
        ("Month Starting", "date"),
        ("Jobs Completed", "total_jobs"),
        ("Core Hours", "core_hours"),
        ("Files Transferred", "files_transferred"),
        ("Unique Users", "users"),
        ("Unique Projects", "projects"),
        ("Unique Institutions Benefiting", "institutions_benefit"),
        ("Unique Institutions Contributing", "institutions_contrib"),
    ])
    dbl_letters = [f"A{x}" for x in ascii_uppercase]
    col_ids = OrderedDict(zip(list(headers.values()), list(ascii_uppercase) + dbl_letters))

    workbook = xlsxwriter.Workbook(str(xlsx_file))
    worksheet = workbook.add_worksheet()

    html = '<html><head></head><body><table style="border-collapse: collapse">\n'

    header_format = workbook.add_format({"text_wrap": True, "align": "center"})
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
    int_format = workbook.add_format({"num_format": "#,##0"})
    float_format = workbook.add_format({"num_format": "#,##0.00"})
    row = 0
    html += "<tr>"
    for col, header in enumerate(headers):
        html += f'<th style="border: 1px solid black">{header}</th>'
        worksheet.write(row, col, header, header_format)
    html += "</tr>\n"
    for i, doc in enumerate(docs):
        row = i+1
        html += "<tr>"
        for col, col_name in enumerate(headers):
            if not (headers[col_name] in doc):
                html += f'<td style="border: 1px solid black"></td>'
                worksheet.write(row, col, "")
            elif (headers[col_name] == "date" and row == 1):  # TOTAL column
                html += f'<td style="border: 1px solid black">TOTAL</td>'
                worksheet.write(row, col, "TOTAL")
            elif (headers[col_name] == "date"):
                date_str = doc[headers[col_name]]
                date = datetime.strptime(date_str, "%Y-%m-%d")
                html += f'<td style="border: 1px solid black">{date_str}</td>'
                worksheet.write(row, col, date, date_format)
            else:
                html += f'<td style="text-align: right; border: 1px solid black">{int(doc[headers[col_name]]):,}</td>'
                worksheet.write(row, col, doc[headers[col_name]], int_format)
        html += "</tr>\n"

    worksheet.set_row(0, 30)
    worksheet.set_column(f"{col_ids['date']}:{col_ids['date']}", 10)
    worksheet.set_column(f"{col_ids['total_jobs']}:{col_ids['files_transferred']}", 13)
    worksheet.set_column(f"{col_ids['users']}:{col_ids['institutions_contrib']}", 9)

    workbook.close()

    html += "</table></body></html>"
    return html


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", action="append")
    return parser.parse_args()


def main():
    args = parse_args()
    to = args.to or TO

    es = elasticsearch.Elasticsearch()
    docs = get_monthly_docs(es)

    datestr = NOW.strftime("%Y-%m-%d")
    xlsx_file = Path() / "1year_summary" / f"{datestr}_OSPool_1Year_Summary.xlsx"
    html = write_xlsx_html(docs.values(), xlsx_file)
    subject = f"{datestr} OSPool 1-Year Summary"
    send_email(from_addr="accounting@chtc.wisc.edu", to_addrs=to, replyto_addr="ospool-reports@path-cc.io", subject=subject, html=html, attachments=[xlsx_file])

if __name__ == "__main__":
    main()
