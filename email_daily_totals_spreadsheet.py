from datetime import datetime, timedelta
from collections import OrderedDict
from string import ascii_uppercase
from pathlib import Path
import elasticsearch
import xlsxwriter
import sys

from email_functions import send_email

es_index_name = "daily_totals"
query_id = "OSG-schedd-job-history"
report_period = "daily"
now = datetime.now()

def get_query(query_id, report_period, now):
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "date": {
                            "gte": (now - timedelta(days=8)).strftime("%Y-%m-%d"),
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
        ("All CPU Hours", "all_cpu_hours"),
        ("% Good CPU Hours", "pct_good_cpu_hours"),
        ("Num Projects", "num_projects"),
        ("Num Users", "num_users"),
        ("Num Sites", "num_sites"),
        ("Num Access Pts", "num_schedds"),
        ("Num Jobs", "num_uniq_job_ids"),
        ("% Rm'd Jobs", "pct_rmed_jobs"),
        ("% Jobs w/ 1+ Holds", "pct_jobs_with_1+_holds"),
        ("% Jobs w/ 2+ Exec Att", "pct_jobs_with_more_than_1_exec_att"),
        ("% Short Jobs", "pct_short_jobs"),
        ("Shadow Starts / Job Id", "shadw_starts_per_job_id"),
        ("Exec Att / Shadow Start", "exec_atts_per_shadw_start"),
        ("Holds / Job Id", "holds_per_job_id"),
        ("CPU Hours / Bad Exec Att", "cpu_hours_per_bad_exec_att"),
        ("25th % Hours", "25pct_hrs"),
        ("Median Hours", "med_hrs"),
        ("75th % Hours", "75pct_hrs"),
        ("95th % Hours", "95pct_hrs"),
        ("Max Hours", "max_hrs"),
        ("Mean Hours", "mean_hrs"),
        ("Std Dev Hours", "std_hrs")
    ])
    col_ids = OrderedDict(zip(list(headers.keys()), ascii_uppercase))

    workbook = xlsxwriter.Workbook(str(xlsx_file))
    worksheet = workbook.add_worksheet()

    html = "<html><head></head><body><table>\n"

    header_format = workbook.add_format({"text_wrap": True, "align": "center"})
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
    int_format = workbook.add_format({"num_format": "#,##0"})
    float_format = workbook.add_format({"num_format": "#,##0.00"})
    pct_format = workbook.add_format({"num_format": "#,##0.00%"})
    hour_format = workbook.add_format({"num_format": "[h]:mm"})
    row = 0
    html += "<tr>"
    for col, header in enumerate(headers):
        html += f"<th>{header}</th>"
        worksheet.write(row, col, header, header_format)
    html += "</tr>\n"
    for i, doc in enumerate(docs):
        row = i+1
        html += "<tr>"
        for col, col_name in enumerate(headers):
            if col_name == "Date":
                date_str = doc[headers[col_name]]
                date = datetime.strptime(date_str, "%Y-%m-%d")
                html += f'<td>{date_str.replace("-", "&#8209;")}</td>'
                worksheet.write(row, col, date, date_format)
            elif (col_name == "All CPU Hours") or (col_name[0:3] == "Num"):
                html += f'<td style="text-align: right">{int(doc[headers[col_name]]):,}</td>'
                worksheet.write(row, col, doc[headers[col_name]], int_format)
            elif " / " in col_name:
                html += f'<td style="text-align: right">{doc[headers[col_name]]:.2f}</td>'
                worksheet.write(row, col, doc[headers[col_name]], float_format)
            elif col_name[0] == "%":
                html += f'<td style="text-align: right">{doc[headers[col_name]]:.2f}%</td>'
                worksheet.write(row, col, doc[headers[col_name]]/100, pct_format)
            elif col_name[-5:] == "Hours":
                h = int(doc[headers[col_name]])
                m = int(60 * (doc[headers[col_name]] - h))
                html += f'<td style="text-align: right">{h}:{m:02d}</td>'
                worksheet.write(row, col, doc[headers[col_name]]/24, hour_format)
            else:
                html += f'<td>{doc[headers[col_name]]}</td>'
                worksheet.write(row, col, doc[headers[col_name]])
        html += "</tr>\n"

    worksheet.set_row(0, 30)
    worksheet.set_column(f"{col_ids['Date']}:{col_ids['Date']}", 10)
    worksheet.set_column(f"{col_ids[list(headers.keys())[1]]}:{col_ids[list(headers.keys())[-1]]}", 10)

    workbook.close()

    html += "</table></body></html>"
    return html
    

def main():
    xlsx_file = Path() / "daily_totals_sheets" / now.strftime("%Y-%m-%d_Weekly_Summary.xlsx")
    es = elasticsearch.Elasticsearch()
    query = get_query(query_id, report_period, now)
    docs = do_query(es, es_index_name, query)
    docs.sort(key = lambda x: datetime.strptime(x["date"], "%Y-%m-%d"))
    html = write_xlsx_html(docs, xlsx_file)
    subject = f"Weekly OSPool Totals Summary from {(now - timedelta(days=8)).strftime('%Y-%m-%d')} to {(now - timedelta(days=1)).strftime('%Y-%m-%d')}"
    send_email(from_addr="accounting@chtc.wisc.edu", to_addrs=["jpatton@cs.wisc.edu"], replyto_addr="jpatton@cs.wisc.edu", subject=subject, html=html, attachments=[xlsx_file])

if __name__ == "__main__":
    main()
