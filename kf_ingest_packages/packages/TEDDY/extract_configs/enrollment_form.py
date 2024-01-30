"""
Extract config module for TEDDY - ENROLLMENT_FORM.CSV.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.etl.extract.operations import keep_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT

source_data_url = "file://../data/ENROLLMENT_FORM.csv"


def do_after_read(df):
    return df[df["Enroll"] == "1"]


operations = [
    keep_map(in_col="MaskID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="enroll_agedays", out_col=CONCEPT.PARTICIPANT.ENROLLMENT_AGE_DAYS),
]
