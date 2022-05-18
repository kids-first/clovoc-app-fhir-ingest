"""
Extract config module for vur - participant details.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map

source_data_url = "file://../data/vesico-ureteric-reflux/vurParticipantDetails.tsv"


operations = [
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="Sex", out_col=CONCEPT.PARTICIPANT.SEX),
    keep_map(in_col="Race", out_col=CONCEPT.PARTICIPANT.RACE),
    value_map(
        in_col="Age at Study Enrollment Value",
        m=lambda x: int(x),
        out_col=CONCEPT.PARTICIPANT.ENROLLMENT_AGE.VALUE,
    ),
    keep_map(
        in_col="Age at Study Enrollment Units",
        out_col=CONCEPT.PARTICIPANT.ENROLLMENT_AGE.UNITS,
    ),
    keep_map(in_col="Species", out_col=CONCEPT.PARTICIPANT.SPECIES),
    keep_map(in_col="Last Known Vital Status", out_col=CONCEPT.OUTCOME.VITAL_STATUS),
]
