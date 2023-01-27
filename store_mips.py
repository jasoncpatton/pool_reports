import htcondor
from datetime import datetime
from collections import defaultdict
from operator import itemgetter
import elasticsearch
import json

from pull_topology import get_mappings

pool = "cm-1.ospool.osg-htc.org,cm-2.ospool.osg-htc.org"
pool_name = "OSPool"
mips_threshold = 11800

es_index_name = "mips_report"

now = datetime.now()

def get_mips():
    mappings = get_mappings()
    collector = htcondor.Collector(pool)
    startd_ads = collector.query(htcondor.AdTypes.Startd, projection=["GLIDEIN_ResourceName", "Mips", "Cpus", "Has_Singularity"])

    resource_cores = defaultdict(int)
    site_cores = defaultdict(int)
    facility_cores = defaultdict(int)
    non_singularity_resources = set()
    non_singularity_sites = set()
    non_singularity_facilities = set()
    mips = []
    has_singularity = []
    for ad in startd_ads:
        if not ("Mips" in ad) or not ("Cpus" in ad):
            continue
        try:
            resource = ad["GLIDEIN_ResourceName"]
            site = mappings["site"].get(resource, f"Unmapped resource {resource}")
            facility = mappings["facility"].get(resource, f"Unmapped resource {resource}")

            int(ad["Mips"])
            cores = int(ad["Cpus"])

            if cores > 0:
                resource_cores[resource] += cores
                site_cores[site] += cores
                facility_cores[facility] += cores

            if not ad.get("Has_Singularity", False):
                non_singularity_resources.add(resource)
                non_singularity_sites.add(site)
                non_singularity_facilities.add(facility)

        except ValueError:
            continue

        for i in range(cores):
            try:
                has_singularity.append(int(ad.get("Has_Singularity", False) == True))
            except Exception:
                pass
            mips.append(ad["Mips"])

    return mips, resource_cores, site_cores, facility_cores, non_singularity_resources, non_singularity_sites, non_singularity_facilities, has_singularity


def get_mips_summary(mips, resource_cores, site_cores, facility_cores, non_singularity_resources, non_singularity_sites, non_singularity_facilities, has_singularity):
    mips.sort()

    total_slow_mips = 0
    for slow_mips_i, m in enumerate(mips):
        if m >= mips_threshold:
            break

    total_slow_mips = sum(mips[:slow_mips_i])
    total_mips = sum(mips)
    total_has_singularity = sum(has_singularity)

    resources_by_core_count = [k for k, v in sorted(resource_cores.items(), key=itemgetter(1), reverse=True)]
    facilities_by_core_count = [k for k, v in sorted(facility_cores.items(), key=itemgetter(1), reverse=True)]
    sites_by_core_count = [k for k, v in sorted(site_cores.items(), key=itemgetter(1), reverse=True)]

    mips_summary = {
        "date": now.strftime("%Y-%m-%d %H:%M:%S"),
        "pool": pool,
        "pool_name": pool_name,
        "mips_threshold": mips_threshold,
        "total_facilities": len(facility_cores),
        "total_sites": len(site_cores),
        "top_3_core_resources": ",".join(resources_by_core_count[:3]),
        "top_3_core_facilities": ",".join(facilities_by_core_count[:3]),
        "top_3_core_sites": ",".join(sites_by_core_count[:3]),
        "total_non_singularity_resources": len(non_singularity_resources),
        "total_non_singularity_sites": len(non_singularity_sites),
        "total_non_singularity_facilities": len(non_singularity_facilities),
        "non_singularity_resources": ",".join(sorted(list(non_singularity_resources))),
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


def get_expanse():
    collector = htcondor.Collector(pool)
    expanse_ads = collector.query(
        htcondor.AdTypes.Startd,
        constraint='GLIDEIN_ResourceName == "Expanse-PATH-EP" && PartitionableSlot',
        projection=["Cpus", "Memory", "Disk"]
    )

    # "Almost exhausted" EP has less than 1 core, 2GB RAM, or 5GB Disk unclaimed
    # "Completely exhausted" EP has less than 1 core, 1GB RAM, or 5GB Disk unclaimed

    total_expanse_eps = len(expanse_ads)
    cpus_exhausted = sum(ad["Cpus"] < 1 for ad in expanse_ads)
    disk_exhausted = sum(ad['Disk'] < (5*1024*1024) for ad in expanse_ads)
    mem_almost_exhausted = sum(ad['Memory'] < (2*1024) for ad in expanse_ads)
    mem_completely_exhausted = sum(ad['Memory'] < (1*1024) for ad in expanse_ads)
    almost_exhausted_expanse_eps = max([cpus_exhausted, disk_exhausted, mem_almost_exhausted])
    completely_exhausted_expanse_eps = max([cpus_exhausted, disk_exhausted, mem_completely_exhausted])

    return(total_expanse_eps, cpus_exhausted, disk_exhausted, mem_almost_exhausted, mem_completely_exhausted, almost_exhausted_expanse_eps, completely_exhausted_expanse_eps)


def get_expanse_summary(total_expanse_eps, cpus_exhausted, disk_exhausted, mem_almost_exhausted, mem_completely_exhausted, almost_exhausted_expanse_eps, completely_exhausted_expanse_eps):
    expanse_summary = {
        "total_expanse_eps": total_expanse_eps,
        "cpus_exhausted": cpus_exhausted,
        "disk_exhausted": disk_exhausted,
        "mem_almost_exhausted": mem_almost_exhausted,
        "mem_completely_exhausted": mem_completely_exhausted,
        "almost_exhausted_expanse_eps": almost_exhausted_expanse_eps,
        "completely_exhausted_expanse_eps": completely_exhausted_expanse_eps,

        "pct_expanse_exhausted": 100 * completely_exhausted_expanse_eps / total_expanse_eps,
        "pct_expanse_almost_exhausted": 100 * almost_exhausted_expanse_eps / total_expanse_eps,
        "pct_expanse_cpus_exhausted": 100 * cpus_exhausted / total_expanse_eps,
        "pct_expanse_mem_exhausted": 100 * mem_completely_exhausted / total_expanse_eps,
        "pct_expanse_mem_almost_exhausted": 100 * mem_almost_exhausted / total_expanse_eps,
        "pct_expanse_disk_exhausted": 100 * disk_exhausted / total_expanse_eps,
    }
    return expanse_summary


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

    data_objs = get_mips()
    mips_summary = get_mips_summary(*data_objs)

    expanse_objs = get_expanse()
    expanse_summary = get_expanse_summary(*expanse_objs)

    total_summary = mips_summary.copy()
    total_summary.update(expanse_summary)

    push_mips_summary(total_summary)
    #print(total_summary)

if __name__ == "__main__":
    main()
