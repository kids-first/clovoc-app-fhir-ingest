"""
Extract config module for vur - biospecimen collection manifest.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map

source_data_url = (
    "file://../data/vesico-ureteric-reflux/BiospecimenCollectionManifest.tsv"
)


operations = [
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="Specimen ID", out_col=CONCEPT.BIOSPECIMEN.ID),
    keep_map(in_col="Specimen Type Name", out_col="BIOSPECIMEN|TYPE|NAME"),
    keep_map(
        in_col="Specimen Type Ontology URI", out_col="BIOSPECIMEN|TYPE|ONTOLOGY_URI"
    ),
    keep_map(in_col="Specimen Type Code", out_col="BIOSPECIMEN|TYPE|ONTOLOGY_CODE"),
    value_map(
        in_col="Age at Collection Value",
        m=lambda x: int(x),
        out_col=CONCEPT.BIOSPECIMEN.EVENT_AGE.VALUE,
    ),
    keep_map(
        in_col="Age at Collection Units", out_col=CONCEPT.BIOSPECIMEN.EVENT_AGE.UNITS
    ),
    keep_map(
        in_col="Method of Sample Procurement",
        out_col=CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT,
    ),
]
