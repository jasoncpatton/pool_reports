from datetime import datetime, timedelta
from collections import OrderedDict
from string import ascii_uppercase
from pathlib import Path
import argparse
import elasticsearch
import xlsxwriter
import sys

from email_functions import send_email

TO = [
    "ospool-reports@path-cc.io",
    "gthain@cs.wisc.edu",
]
DAYS = 30
ES_INDEX_NAME = "mips_report"
POOL_NAME = "OSPool"

def get_query(pool_name, days, now):
    query = {
        "size": days*4,
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "date": {
                            "gte": (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
                            "lt": now.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    }},
                    {"term": {
                        "pool_name": {
                            "value": pool_name,
                        }
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
    headers = ["Date", "Num Sites", "Top Core Sites", "Total Cores", "Total MIPS", "Total Singularity Cores",
                   "Sites w/o S'ty", "% Slow MIPS", "% Slow Cores", "% Singularity Cores",
                   "Mean MIPS", "Median MIPS", "Min MIPS", "Max MIPS",
                   "Slow MIPS", "Slow Cores",]
    col_ids = OrderedDict(zip(headers, ascii_uppercase))
    header_key = {
        "Date": "date",
        "Num Sites": "total_sites",
        "Min MIPS": "min_mips",
        "Mean MIPS": "mean_mips",
        "Median MIPS": "median_mips",
        "Max MIPS": "max_mips",
        "Total MIPS": "total_mips",
        "Slow MIPS": "total_slow_mips",
        "Total Cores": "total_cores",
        "Slow Cores": "slow_cores",
        "Total Singularity Cores": "total_singularity_cores",
        "Sites w/o S'ty": "total_non_singularity_sites",
        "Top Core Sites": "top_3_core_sites",
    }

    workbook = xlsxwriter.Workbook(str(xlsx_file))
    worksheet = workbook.add_worksheet()

    html = '<html><head></head><body><table style="border-collapse: collapse">\n'

    header_format = workbook.add_format({"text_wrap": True, "align": "center"})
    date_format = workbook.add_format({"num_format": "mm-dd h AM/PM"})
    int_format = workbook.add_format({"num_format": "#,##0"})
    pct_format = workbook.add_format({"num_format": "#,##0.00%"})
    text_format = workbook.add_format({"text_wrap": True, "align": "left"})
    row = 0
    html += "<tr>"
    for col, header in enumerate(headers):
        html += f'''<th style="border: 1px solid black">{header.replace("Singularity", "S'ty")}</th>'''
        worksheet.write(row, col, header, header_format)
    html += "</tr>\n"
    for i, doc in enumerate(docs):
        row = i+1
        html += "<tr>"
        for col, col_name in enumerate(col_ids.keys()):
            if col_name == "Date":
                date_str = doc[header_key[col_name]]
                date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                html += f'<td style="border: 1px solid black">{date.strftime("%m-%d %H:%M")}</td>'
                worksheet.write(row, col, date, date_format)
            elif col_name == "Top Core Sites":
                if header_key[col_name] in doc:
                    # add a zero-width space after comma in HTML to allow line wrapping
                    html += f'<td style="text-align: left; border: 1px solid black">{doc[header_key[col_name]].replace(",", ",&#8203;")}</td>'
                    worksheet.write(row, col, doc[header_key[col_name]], text_format)
                else:
                    html += f'<td style="text-align: left; border: 1px solid black"></td>'
                    worksheet.write(row, col, "", text_format)
            elif col_name in header_key:
                if header_key[col_name] in doc:
                    html += f'<td style="text-align: right; border: 1px solid black">{int(doc[header_key[col_name]]):,}</td>'
                    worksheet.write(row, col, doc[header_key[col_name]], int_format)
                else:
                    html += f'<td style="text-align: right; border: 1px solid black"></td>'
                    worksheet.write(row, col, "", int_format)
            elif col_name == "% Slow MIPS":
                slow_mips = 100 * doc[header_key["Slow MIPS"]] / doc[header_key["Total MIPS"]]
                html += f'<td style="text-align: right; border: 1px solid black">{slow_mips:.2f}%</td>'
                formula = f"={col_ids['Slow MIPS']}{row+1}/{col_ids['Total MIPS']}{row+1}"
                worksheet.write(row, col, formula, pct_format)
            elif col_name == "% Slow Cores":
                slow_cores = 100 * doc[header_key["Slow Cores"]] / doc[header_key["Total Cores"]]
                html += f'<td style="text-align: right; border: 1px solid black">{slow_cores:.2f}%</td>'
                formula = f"={col_ids['Slow Cores']}{row+1}/{col_ids['Total Cores']}{row+1}"
                worksheet.write(row, col, formula, pct_format)
            elif col_name == "% Singularity Cores":
                if header_key["Total Singularity Cores"] in doc:
                    singularity_cores = 100 * doc[header_key["Total Singularity Cores"]] / doc[header_key["Total Cores"]]
                    html += f'<td style="text-align: right; border: 1px solid black">{singularity_cores:.2f}%</td>'
                    formula = formula = f"={col_ids['Total Singularity Cores']}{row+1}/{col_ids['Total Cores']}{row+1}"
                else:
                    html += f'<td style="text-align: right; border: 1px solid black"></td>'
                    formula = ""
                worksheet.write(row, col, formula, pct_format)

        html += "</tr>\n"

    row = row+2
    worksheet.write(row, 0, "Slow MIPS Threshold")
    worksheet.write(row, 1, doc["mips_threshold"])

    worksheet.set_row(0, 15)
    worksheet.set_column(f"{col_ids['Date']}:{col_ids['Date']}", 16)
    worksheet.set_column(f"{col_ids[headers[1]]}:{col_ids[headers[-1]]}", 12)

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
    xlsx_file = Path() / "mips_sheets" / now.strftime(f"%Y-%m-%d_{days}day_MIPS_Report.xlsx")
    es = elasticsearch.Elasticsearch()
    query = get_query(POOL_NAME, days, now)
    docs = do_query(es, ES_INDEX_NAME, query)
    docs.sort(key = lambda x: datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S"), reverse=True)
    html = write_xlsx_html(docs, xlsx_file)
    subject = f"{days}-day {POOL_NAME} MIPS Summary from {(now - timedelta(days=days)).strftime('%Y-%m-%d')} to {(now - timedelta(days=1)).strftime('%Y-%m-%d')}"
    send_email(from_addr="accounting@chtc.wisc.edu", to_addrs=to, replyto_addr="ospool-reports@path-cc.io", subject=subject, html=html, attachments=[xlsx_file])

if __name__ == "__main__":
    main()
