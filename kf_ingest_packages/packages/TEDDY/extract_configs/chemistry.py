"""
Extract config module for TEDDY - TEST_RESULTS (CHEMISTRY: HbA1c, OGTTPEP, GLU, OGTTINS, OGTTGLU).

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

import os

import pandas as pd

from kf_lib_data_ingest.common.file_retriever import FileRetriever as FR
from kf_lib_data_ingest.common.io import read_df
from kf_lib_data_ingest.etl.extract.operations import keep_map, constant_map, value_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common import constants

# Dummy URL
source_data_url = "file://../data/clinical.tsv"

file_name_list = [
    "TEST_RESULTS - HbA1c.csv",
    "TEST_RESULTS - OGTTPEP.csv",
    "TEST_RESULTS - GLU.csv",
    "TEST_RESULTS - OGTTINS.csv",
    "TEST_RESULTS - OGTTGLU.csv",
]


def do_after_read(dummy_df):
    package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(package_dir, "data")

    filtered_records = []
    for file_name in file_name_list:
        file_path = f"file://{os.path.join(data_dir, file_name)}"
        for _, row in read_df(FR().get(file_path)).iterrows():
            if row.get("result"):
                filtered_records.append(row)

    return pd.DataFrame(filtered_records)


operations = [
    keep_map(in_col="test_name", out_col=CONCEPT.OBSERVATION.NAME),
    constant_map(
        m="http://loinc.org", out_col=CONCEPT.OBSERVATION.ONTOLOGY_ONTOBEE_URI
    ),
    keep_map(in_col="LOINC_code", out_col=CONCEPT.OBSERVATION.ONTOLOGY_CODE),
    keep_map(in_col="result", out_col="OBSERVATION|QUANTITY|VALUE"),
    keep_map(in_col="result_unit", out_col="OBSERVATION|QUANTITY|UNITS"),
    value_map(
        m=lambda x: int(int(x) / 30.44),
        in_col="draw_age",
        out_col=CONCEPT.OBSERVATION.EVENT_AGE.VALUE,
    ),
    constant_map(
        m=constants.AGE.UNITS.MONTHS, out_col=CONCEPT.OBSERVATION.EVENT_AGE.UNITS
    ),
    keep_map(in_col="maskid", out_col=CONCEPT.PARTICIPANT.ID),
]
