"""
Extract config module for Kids First - participant details.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, constant_map

source_data_url = "file://../data/kids-first/ParticipantDetails.tsv"


operations = [
    constant_map(m="kids-first", out_col=CONCEPT.STUDY.ID),
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="Sex", out_col=CONCEPT.PARTICIPANT.SEX),
    keep_map(in_col="Race", out_col=CONCEPT.PARTICIPANT.RACE),
    keep_map(in_col="Ethnicity", out_col=CONCEPT.PARTICIPANT.ETHNICITY),
    keep_map(in_col="Last Known Vital Status", out_col=CONCEPT.OUTCOME.VITAL_STATUS),
]
