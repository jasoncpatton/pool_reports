import pickle
from pathlib import Path

OSPOOL_AP_COLLECTOR_HOST_MAP_FILE = Path().home() / "JobAccounting" / "ospool-host-map.pkl"
OSPOOL_COLLECTORS = {
    "cm-1.ospool.osg-htc.org",
    "cm-2.ospool.osg-htc.org",
    "flock.opensciencegrid.org",
}

def get_ospool_aps():
    aps = set()
    ap_collector_host_map = pickle.load(open(OSPOOL_AP_COLLECTOR_HOST_MAP_FILE, "rb"))
    for ap, collectors in ap_collector_host_map.items():
        if ap.startswith("jupyter-notebook-") or ap.startswith("jupyterlab-"):
            continue
        if len(collectors & OSPOOL_COLLECTORS) > 0:
            aps.add(ap)
    return aps


if __name__ == "__main__":
    print(sorted(list(get_ospool_aps())))
