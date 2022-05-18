"""
Extract config module for vur - participant phenotypes.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map

source_data_url = "file://../data/vesico-ureteric-reflux/ParticipantPhenotypes.tsv"


def to_safe_int(x):
    try:
        return int(x)
    except:
        return None


operations = [
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    value_map(
        in_col="Condition Prevalence Duration Value",
        m=to_safe_int,
        out_col="PHENOTYPE|EVENT_DURATION|VALUE",
    ),
    keep_map(
        in_col="Condition Prevalence Duration Units",
        out_col="PHENOTYPE|EVENT_DURATION|UNITS",
    ),
    keep_map(in_col="Group", out_col="GROUP|NAME"),
    keep_map(in_col="Condition Name", out_col=CONCEPT.PHENOTYPE.NAME),
    keep_map(in_col="Condition Ontology URI", out_col="PHENOTYPE|ONTOLOGY_URI"),
    keep_map(in_col="Condition Code", out_col="PHENOTYPE|ONTOLOGY_CODE"),
    keep_map(in_col="Verification Status", out_col=CONCEPT.PHENOTYPE.VERIFICATION),
    keep_map(in_col="Body Site Name", out_col="PHENOTYPE|BODY_SITE|NAME"),
    keep_map(
        in_col="Body Site Ontology URI", out_col="PHENOTYPE|BODY_SITE|ONTOLOGY_URI"
    ),
    keep_map(in_col="Body Site Code", out_col="PHENOTYPE|BODY_SITE|ONTOLOGY_CODE"),
]
