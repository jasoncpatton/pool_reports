import sys
import json
import argparse
import importlib

from datetime import datetime, timedelta
from pathlib import Path
from operator import itemgetter

from functions import get_ospool_aps, send_email

import elasticsearch
from elasticsearch_dsl import Search, A


EMAIL_ARGS = {
    "--from": {"dest": "from_addr", "default": "no-reply@chtc.wisc.edu"},
    "--reply-to": {"default": "ospool-reports@g-groups.wisc.edu"},
    "--to": {"action": "append", "default": []},
    "--cc": {"action": "append", "default": []},
    "--bcc": {"action": "append", "default": []},
    "--smtp-server": {},
    "--smtp-username": {},
    "--smtp-password-file": {"type": Path}
}

ELASTICSEARCH_ARGS = {
    "--es-host": {},
    "--es-url-prefix": {},
    "--es-index": {},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    }
}


def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    email_args = parser.add_argument_group("email-related options")
    for name, properties in EMAIL_ARGS.items():
        email_args.add_argument(name, **properties)

    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    parser.add_argument("--start", type=valid_date)
    parser.add_argument("--end", type=valid_date)

    return parser.parse_args()


def connect(
        es_host="localhost:9200",
        es_user="",
        es_pass="",
        es_use_https=False,
        es_ca_certs=None,
        es_url_prefix=None,
        **kwargs,
    ) -> elasticsearch.Elasticsearch:
    # Returns Elasticsearch client

    # Split off port from host if included
    if ":" in es_host and len(es_host.split(":")) == 2:
        [es_host, es_port] = es_host.split(":")
        es_port = int(es_port)
    elif ":" in es_host:
        print(f"Ambiguous hostname:port in given host: {es_host}")
        sys.exit(1)
    else:
        es_port = 9200
    es_client = {
        "host": es_host,
        "port": es_port
    }

    # Include username and password if both are provided
    if (not es_user) ^ (not es_pass):
        print("Only one of es_user and es_pass have been defined")
        print("Connecting to Elasticsearch anonymously")
    elif es_user and es_pass:
        es_client["http_auth"] = (es_user, es_pass)

    if es_url_prefix:
        es_client["url_prefix"] = es_url_prefix

    # Only use HTTPS if CA certs are given or if certifi is available
    if es_use_https:
        if es_ca_certs is not None:
            es_client["ca_certs"] = str(es_ca_certs)
        elif importlib.util.find_spec("certifi") is not None:
            pass
        else:
            print("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
            sys.exit(1)
        es_client["use_ssl"] = True
        es_client["verify_certs"] = True

    return elasticsearch.Elasticsearch([es_client])


def get_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime
    ) -> Search:
    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .filter("term", period_days=1) \
                .filter("range", timestamp_start={"gte": int(start.timestamp()), "lt": int(end.timestamp())})
    schedd_name_agg = A(
        "terms",
        field="schedd",
        size=64,
    )
    num_ads_agg = A(
        "sum",
        field="num_ads"
    )
    num_seen_agg = A(
        "sum",
        field="num_seen"
    )
    num_failures_agg = A(
        "sum",
        field="num_failures",
    )
    most_common_failure_agg = A(
        "terms",
        field="most_common_failure",
        missing="none",
        size=2,
    )
    schedd_name_agg.metric("num_ads", num_ads_agg) \
                   .metric("num_seen", num_seen_agg) \
                   .metric("num_failures", num_failures_agg) \
                   .metric("most_common_failure", most_common_failure_agg)
    query.aggs.bucket("schedds", schedd_name_agg)
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


def main():
    args = parse_args()
    es_args = {}
    if args.es_config_file:
        es_args = json.load(args.es_config_file.open())
    else:
        es_args = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
    if es_args.get("es_password_file"):
        es_args["es_pass"] = es_args["es_password_file"].open().read().rstrip()
    index = es_args.get("es_index", "ap-stats")

    if args.start is None:
        args.start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.end is None:
        args.end = args.start + timedelta(days=1)

    ospool_aps = get_ospool_aps()

    es = connect(**es_args)
    es.info()
    query = get_query(es, index, start=args.start, end=args.end)

    try:
        result = query.execute()
    except Exception as err:
        try:
            print_error(err.info)
        except Exception:
            pass
        raise err

    rows = []
    for schedd_bucket in result.aggregations.schedds.buckets:
        row = {
            "schedd": schedd_bucket["key"],
            "num_ads": schedd_bucket["num_ads"]["value"],
            "num_seen": schedd_bucket["num_seen"]["value"],
            "num_failures": schedd_bucket["num_failures"]["value"],
            "most_common_failure": schedd_bucket["most_common_failure"]["buckets"][0]["key"].rstrip("."),
            "ospool_ap": False,
        }
        if len(schedd_bucket["most_common_failure"]["buckets"]) > 1 and row["most_common_failure"] == "none":
            row["most_common_failure"] = schedd_bucket["most_common_failure"]["buckets"][1]["key"].rstrip(".")
        row["ads_per_query"] = row["num_ads"] / max(row["num_seen"], 1)
        row["ads_per_hour"] = row["num_ads"] / ((args.end - args.start).total_seconds() / 3600)
        row["failure_rate"] = min(row["num_failures"] / max(row["num_seen"], 1), 1.)
        if row["most_common_failure"] == "none" and row["num_failures"] > 0:
            row["most_common_failure"] = "unknown"
        elif row["num_failures"] == 0:
            row["most_common_failure"] = ""
        elif row["most_common_failure"].startswith("Failed to connect to"):
            row["most_common_failure"] = "Failed to connect to schedd"
        elif row["most_common_failure"].startswith('Received "DENIED"'):
            row["most_common_failure"] = "Permission denied"
        if row["schedd"] in ospool_aps:
            row["ospool_ap"] = True
        rows.append(row)

    rows.sort(key=itemgetter("failure_rate", "ads_per_hour", "num_seen"), reverse=True)

    needs_attention = 0
    lines = []
    lines.append(f"{'AP hostname (* if flocking to OSPool)':<42} {'fail%':>6} {'ads/h':>8} {'tries':>5} most common failure")
    for row in rows:
        if row["failure_rate"] > 0.1:
            needs_attention += 1
        lines.append(f"{row['schedd'] + ('*' if not row['ospool_ap'] else ''):<42} {row['failure_rate']:>6.1%} {row['ads_per_hour']:>8,.1f} {int(row['num_seen']):>5,d} {row['most_common_failure']}")

    lf = "\n"
    html = f'<pre style="font-family: monospace; white-space: pre">\n<strong>{lines[0]}</strong>\n{lf.join(lines[1:])}\n</pre>'
    days = (args.end - args.start).days
    subject = f"{args.end.strftime(r'%Y-%m-%d')} {days}-day OSPool APs adstash query report ({needs_attention} problems)"
    send_email(
        subject=subject,
        from_addr=args.from_addr,
        to_addrs=args.to,
        html=html,
        cc_addrs=args.cc,
        bcc_addrs=args.cc,
        reply_to_addr=args.reply_to,
        smtp_server=args.smtp_server,
        smtp_username=args.smtp_username,
        smtp_password_file=args.smtp_password_file,
    )


if __name__ == "__main__":
    main()
