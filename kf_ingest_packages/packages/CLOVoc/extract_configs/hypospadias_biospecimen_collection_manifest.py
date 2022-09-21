"""
Extract config module for hypospadias - biospecimen collection manifest.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map

source_data_url = "file://../data/hypospadias/BiospecimenCollectionManifest.tsv"


operations = [
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="Specimen ID", out_col=CONCEPT.BIOSPECIMEN.ID),
    keep_map(in_col="Specimen Type Name", out_col="BIOSPECIMEN|TYPE|NAME"),
    keep_map(
        in_col="Specimen Type Ontology URI", out_col="BIOSPECIMEN|TYPE|ONTOLOGY_URI"
    ),
    keep_map(in_col="Specimen Type Code", out_col="BIOSPECIMEN|TYPE|ONTOLOGY_CODE"),
    keep_map(in_col="Body Site Name", out_col="BIOSPECIMEN|BODY_SITE|NAME"),
    keep_map(
        in_col="Body Site Ontology URI", out_col="BIOSPECIMEN|BODY_SITE|ONTOLOGY_URI"
    ),
    keep_map(in_col="Body Site Code", out_col="BIOSPECIMEN|BODY_SITE|ONTOLOGY_CODE"),
]
