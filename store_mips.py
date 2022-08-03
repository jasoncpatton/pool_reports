import htcondor
from datetime import datetime
import elasticsearch
import json

pool = "cm-1.ospool.osg-htc.org,cm-2.ospool.osg-htc.org"
pool_name = "OSPool"
mips_threshold = 11800

es_index_name = "mips_report"

now = datetime.now()

def get_mips():
    collector = htcondor.Collector(pool)
    startd_ads = collector.query(htcondor.AdTypes.Startd, projection=["Mips", "Cpus", "Has_Singularity"])

    mips = []
    has_singularity = []
    for ad in startd_ads:
        if not ("Mips" in ad) or not ("Cpus" in ad):
            continue
        try:
            int(ad["Mips"])
            int(ad["Cpus"])
        except ValueError:
            continue
        for i in range(ad["Cpus"]):
            try:
                has_singularity.append(int(ad.get("Has_Singularity", False) == True))
            except Exception:
                pass
            mips.append(ad["Mips"])

    return mips, has_singularity

def get_mips_summary(mips, has_singularity):
    mips.sort()

    total_slow_mips = 0
    for slow_mips_i, m in enumerate(mips):
        if m >= mips_threshold:
            break

    total_slow_mips = sum(mips[:slow_mips_i])
    total_mips = sum(mips)
    total_has_singularity = sum(has_singularity)

    mips_summary = {
        "date": now.strftime("%Y-%m-%d %H:%M:%S"),
        "pool": pool,
        "pool_name": pool_name,
        "mips_threshold": mips_threshold,
        "min_mips": mips[0],
        "max_mips": mips[-1],
        "mean_mips": int(total_mips/len(mips)),
        "median_mips": mips[len(mips)//2],
        "total_cores": len(mips),
        "slow_cores": slow_mips_i,
        "total_slow_mips": total_slow_mips,
        "total_mips": total_mips,
        "pct_slow_mips": 100*total_slow_mips/total_mips,
        "total_singularity_cores": total_has_singularity,
        "pct_singularity_cores": 100*total_has_singularity/len(has_singularity),
    }
    return mips_summary

def push_mips_summary(mips_summary):
    es = elasticsearch.Elasticsearch()
    #index_client = elasticsearch.client.IndicesClient(es)
    if not es.indices.exists(es_index_name):
        properties = {
            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
        }
        dynamic_templates = [
            {
                "strings_as_keywords": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword", "norms": "false", "ignore_above": 256},
                }
            },
        ]
        mappings = {
            "dynamic_templates": dynamic_templates,
            "properties": properties,
            "date_detection": False,
            "numeric_detection": True,
        }
        body = json.dumps({"mappings": mappings})
        es.indices.create(index=es_index_name, body=body)
    doc_id = f"{mips_summary['pool']}_{mips_summary['date']}"
    es.index(index=es_index_name, id=doc_id, body=mips_summary)

def main():

    mips, has_singularity = get_mips()
    mips_summary = get_mips_summary(mips, has_singularity)
    push_mips_summary(mips_summary)

if __name__ == "__main__":
    main()
