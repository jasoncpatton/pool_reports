from datetime import datetime, timedelta
from collections import OrderedDict
from string import ascii_uppercase
from pathlib import Path
import argparse
import elasticsearch
import xlsxwriter
import sys

from email_functions import send_email

TO = ["ospool-reports@path-cc.io"]
DAYS = 30
ES_INDEX_NAME = "daily_totals"
QUERY_ID = "OSG-schedd-job-history"
REPORT_PERIOD = "daily"


def get_query(query_id, report_period, days, now):
    query = {
        "size": days+1,
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "date": {
                            "gte": (now - timedelta(days=days)).strftime("%Y-%m-%d"),
                            "lt": now.strftime("%Y-%m-%d"),
                        }
                    }},
                    {"term": {
                        "query": {"value": query_id}
                    }},
                    {"term": {
                        "report_period": {"value": report_period}
                    }},
                ]
            }
        }
    }
    return query


def do_query(es, index, body):
    result = es.search(index=index, body=body)
    docs = []
    for hit in result["hits"]["hits"]:
        docs.append(hit["_source"])
    return docs


def write_xlsx_html(docs, xlsx_file):
    headers = OrderedDict([
        ("Date", "date"),
        ("Num Proj", "num_projects"),
        ("Num Users", "num_users"),
        ("Num Insts", "num_institutions"),
        ("Num Sites", "num_sites"),
        ("Num Acc Pts", "num_schedds"),
        ("Num Jobs", "num_uniq_job_ids"),
        ("All CPU Hours", "all_cpu_hours"),
        ("% Good CPU Hours", "pct_good_cpu_hours"),
        ("Job Unit Hours", "job_unit_hours"),
        ("% Ckpt Able", "pct_ckpt_able"),
        ("% Rm'd Jobs", "pct_rmed_jobs"),
        ("Total Files Xferd", "total_files_xferd"),
        ("OSDF Files Xferd", "osdf_files_xferd"),
        ("% OSDF Files", "pct_osdf_files"),
        ("% OSDF Bytes", "pct_osdf_bytes"),
        ("Shadw Starts / Job Id", "shadw_starts_per_job_id"),
        ("Exec Att / Shadw Start", "exec_atts_per_shadw_start"),
        ("Holds / Job Id", "holds_per_job_id"),
        ("% Short Jobs", "pct_short_jobs"),
        ("% Jobs w/ 2+ Exec Att", "pct_jobs_with_more_than_1_exec_att"),
        ("% Jobs w/ 1+ Holds", "pct_jobs_with_1+_holds"),
        ("% Jobs Over Rqst Disk", "pct_jobs_over_rqst_disk"),
        ("% S'ty Jobs", "pct_jobs_using_s'ty"),
        ("Mean Actv Hrs", "mean_actv_hrs"),
        ("Mean Setup Secs", "mean_setup_secs"),
        ("25th % Hrs", "25pct_hrs"),
        ("Med Hrs", "med_hrs"),
        ("75th % Hrs", "75pct_hrs"),
        ("95th % Hrs", "95pct_hrs"),
        ("Max Hrs", "max_hrs"),
        ("Mean Hrs", "mean_hrs"),
        ("Std Dev Hrs", "std_hrs"),
        ("Inpt Files / Exec Att", "input_files_per_exec_att"),
        ("Outpt Files / Job", "output_files_per_job"),
        ("CPU Hrs / Bad Exec Att", "cpu_hours_per_bad_exec_att"),
    ])
    dbl_letters = [f"A{x}" for x in ascii_uppercase]
    col_ids = OrderedDict(zip(list(headers.keys()), list(ascii_uppercase) + dbl_letters))

    workbook = xlsxwriter.Workbook(str(xlsx_file))
    worksheet = workbook.add_worksheet()

    html = '<html><head></head><body><table style="border-collapse: collapse">\n'

    header_format = workbook.add_format({"text_wrap": True, "align": "center"})
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
    int_format = workbook.add_format({"num_format": "#,##0"})
    float_format = workbook.add_format({"num_format": "#,##0.00"})
    pct_format = workbook.add_format({"num_format": "#,##0.00%"})
    hour_format = workbook.add_format({"num_format": "[h]:mm"})
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
            elif col_name == "Date":
                date_str = doc[headers[col_name]]
                date = datetime.strptime(date_str, "%Y-%m-%d")
                html += f'<td style="border: 1px solid black">{date.strftime("%m&#8209;%d")}</td>'
                worksheet.write(row, col, date, date_format)
                # Fix for Site/Facility/Institutions switcheroo
                if date < datetime(2023, 1, 3):
                    headers["Num Insts"] = "num_sites"
                    headers["Num Sites"] = None
                elif date < datetime(2023, 1, 17):
                    headers["Num Insts"] = "num_facilitys"
                    headers["Num Sites"] = "num_sites"
                else:
                    headers["Num Insts"] = "num_institutions"
                    headers["Num Sites"] = "num_sites"
            elif col_name == "Mean Actv Hrs":
                if float(doc[headers[col_name]]) < 0:
                    html += f'<td style="border: 1px solid black"></td>'
                    worksheet.write(row, col, "")
                else:
                    h = int(float(doc[headers[col_name]]))
                    m = int(60 * (float(doc[headers[col_name]]) - h))
                    html += f'<td style="text-align: right; border: 1px solid black">{h}:{m:02d}</td>'
                    worksheet.write(row, col, doc[headers[col_name]]/24, hour_format)
            elif col_name == "Mean Setup Secs":
                if float(doc[headers[col_name]]) < 0:
                    html += f'<td style="border: 1px solid black"></td>'
                    worksheet.write(row, col, "")
                else:
                    html += f'<td style="text-align: right; border: 1px solid black">{int(float(doc[headers[col_name]])):,}</td>'
                    worksheet.write(row, col, int(float(doc[headers[col_name]])), int_format)
            elif (col_name in {"All CPU Hours", "Job Unit Hours"}) or any(col_name.startswith(x) for x in ["Num", "Total", "OSDF"]):
                html += f'<td style="text-align: right; border: 1px solid black">{int(doc[headers[col_name]]):,}</td>'
                worksheet.write(row, col, doc[headers[col_name]], int_format)
            elif " / " in col_name:
                html += f'<td style="text-align: right; border: 1px solid black">{doc[headers[col_name]]:.2f}</td>'
                worksheet.write(row, col, doc[headers[col_name]], float_format)
            elif col_name[0] == "%":
                html += f'<td style="text-align: right; border: 1px solid black">{doc[headers[col_name]]:.2f}%</td>'
                worksheet.write(row, col, doc[headers[col_name]]/100, pct_format)
            elif col_name[-5:] == "Hours" or col_name[-3:] == "Hrs":
                h = int(doc[headers[col_name]])
                m = int(60 * (doc[headers[col_name]] - h))
                html += f'<td style="text-align: right; border: 1px solid black">{h}:{m:02d}</td>'
                worksheet.write(row, col, doc[headers[col_name]]/24, hour_format)
            else:
                html += f'<td style="border: 1px solid black">{doc[headers[col_name]]}</td>'
                worksheet.write(row, col, doc[headers[col_name]])
        html += "</tr>\n"

    worksheet.set_row(0, 30)
    worksheet.set_column(f"{col_ids['Date']}:{col_ids['Date']}", 10)
    worksheet.set_column(f"{col_ids[list(headers.keys())[1]]}:{col_ids[list(headers.keys())[-1]]}", 10)

    workbook.close()

    html += "</table></body></html>"
    return html


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", action="append")
    parser.add_argument("--days", type=int, default=DAYS)
    return parser.parse_args()


def main():
    now = datetime.now()
    args = parse_args()
    days = args.days
    to = args.to or TO
    xlsx_file = Path() / "daily_totals_sheets" / now.strftime(f"%Y-%m-%d_OSPool_{days}day_Summary.xlsx")
    es = elasticsearch.Elasticsearch()
    query = get_query(QUERY_ID, REPORT_PERIOD, days, now)
    docs = do_query(es, ES_INDEX_NAME, query)
    docs.sort(key = lambda x: datetime.strptime(x["date"], "%Y-%m-%d"), reverse=True)
    html = write_xlsx_html(docs, xlsx_file)
    subject = f"{days}-day OSPool Totals Summary from {(now - timedelta(days=days)).strftime('%Y-%m-%d')} to {(now - timedelta(days=1)).strftime('%Y-%m-%d')}"
    send_email(from_addr="accounting@chtc.wisc.edu", to_addrs=to, replyto_addr="ospool-reports@path-cc.io", subject=subject, html=html, attachments=[xlsx_file])


if __name__ == "__main__":
    main()
