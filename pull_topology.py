import pickle
import tempfile
import os
import time
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from pathlib import Path


RESOURCE_SUMMARY_URL = "https://topology.opensciencegrid.org/rgsummary/xml"
TOPOLOGY_PICKLE = Path("topology_map.pkl")


MANUAL_FACILITY_MAPPINGS = {
    "SURFsara": "SURFsara",  # European
    "IN2P3-CC": "IN2P3",  # European
    "NIKHEF-ELPROD": "Nikhef",  # European
    "GPGrid": "Fermi National Accelerator Laboratory",  # Inferred from topology
    "ISI_ImageTest": "University of Southern California",  # Inferred from topology
    "GP-ARGO-doane-backfill": "Great Plains Network",  # Inferred from topology
    "GP-ARGO-cameron-backfill": "Great Plains Network",  # Inferred from topology
    "KSU-Beocat-CE1": "Kansas State University",  # Inferred from topology
}

MANUAL_SITE_MAPPINGS = {
    "SURFsara": "SURFsara",  # European
    "IN2P3-CC": "IN2P3",  # European
    "NIKHEF-ELPROD": "Nikhef",  # European
    "GPGrid": "FermiGrid",  # Inferred from topology
    "ISI_ImageTest": "Information Sciences Institute",  # Inferred from topology
    "GP-ARGO-doane-backfill": "Doane University",  # Inferred from topology
    "GP-ARGO-cameron-backfill": "Cameron University",  # Inferred from topology
    "KSU-Beocat-CE1": "Beocat",  # Inferred from topology
}

MANUAL_GROUP_MAPPINGS = {
    "SURFsara": "SURFsara",  # European
    "IN2P3-CC": "IN2P3",  # European
    "NIKHEF-ELPROD": "Nikhef",  # European
    "GPGrid": "GPGRID",  # Inferred from topology
    "ISI_ImageTest": "ISI",  # Inferred from topology
    "GP-ARGO-doane-backfill": "GP-ARGO-doane",  # Inferred from topology
    "GP-ARGO-cameron-backfill": "GP-ARGO-cameron",  # Inferred from topology
}


def get_latest_mappings():
    """Gets latest mappings from topology XML"""

    xmltree = ET.parse(urlopen(RESOURCE_SUMMARY_URL))
    xmlroot = xmltree.getroot()

    mappings = {
        "group": MANUAL_GROUP_MAPPINGS.copy(),
        "facility": MANUAL_FACILITY_MAPPINGS.copy(),
        "site": MANUAL_SITE_MAPPINGS.copy(),
    }

    for resource_group in xmlroot:
        names = {
            "group": resource_group.find("GroupName").text,
            "facility": resource_group.find("Facility").find("Name").text,
            "site": resource_group.find("Site").find("Name").text,
        }

        for name in names.values():
            for mapping_type in mappings.keys():
                mappings[mapping_type][name] = names[mapping_type]

        resources = resource_group.find("Resources")
        for resource in resources:
            resource_name = resource.find("Name").text
            for mapping_type in mappings.keys():
                mappings[mapping_type][resource_name] = names[mapping_type]

    return mappings


def update_topology_pickle(topology_pickle):
    """Updates (or creates) site map pickle file"""

    mappings = get_latest_mappings()

    # Write atomically
    with tempfile.NamedTemporaryFile(delete=False, dir=str(Path.cwd())) as tf:
        tmpfile = Path(tf.name)
        with tmpfile.open("wb") as f:
            pickle.dump(mappings, f, pickle.HIGHEST_PROTOCOL)
            f.flush()
            os.fsync(f.fileno())
    tmpfile.rename(topology_pickle)


def get_mappings(topology_pickle=TOPOLOGY_PICKLE, force_update=False):
    """Returns mappings"""

    # Update map if older than a day
    try:
        if (time.time() - topology_pickle.stat().st_mtime > 23*3600) or force_update:
            update_topology_pickle(topology_pickle)
        return pickle.load(topology_pickle.open("rb"))
    except FileNotFoundError:
        update_topology_pickle(topology_pickle)
    return pickle.load(topology_pickle.open("rb"))


if __name__ == "__main__":
    mappings = get_mappings(force_update=True)
    for mapping_type, mapping in mappings.items():
        print(f"{mapping_type}:")
        print(mapping)
        print()
