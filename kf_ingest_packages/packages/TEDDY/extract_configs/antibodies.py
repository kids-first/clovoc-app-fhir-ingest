"""
Extract config module for TEDDY - TEST_RESULTS (ANTIBODIES).

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.etl.extract.operations import keep_map, constant_map, value_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common import constants

source_data_url = "file://../data/TEST_RESULTS - ANTIBODIES.csv"

operations = [
    keep_map(in_col="test_name", out_col=CONCEPT.OBSERVATION.NAME),
    constant_map(
        m="http://loinc.org", out_col=CONCEPT.OBSERVATION.ONTOLOGY_ONTOBEE_URI
    ),
    keep_map(in_col="LOINC_code", out_col=CONCEPT.OBSERVATION.ONTOLOGY_CODE),
    keep_map(in_col="result", out_col="OBSERVATION|QUANTITY|VALUE"),
    keep_map(in_col="result_unit", out_col="OBSERVATION|QUANTITY|UNITS"),
    value_map(
        m={"Pos": "Positive"},
        in_col="outcome",
        out_col=CONCEPT.OBSERVATION.INTERPRETATION,
    ),
    keep_map(in_col="antibody_specname", out_col="OBSERVATION|COMPONENT|NAME"),
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
