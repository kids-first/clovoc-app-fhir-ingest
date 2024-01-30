"""
Extract config module for TEDDY - TEST_RESULTS (GENOTYPE).

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.etl.extract.operations import constant_map, keep_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT

source_data_url = "file://../data/TEST_RESULTS - GENOTYPE.csv"

operations = [
    constant_map(
        m="http://loinc.org", out_col=CONCEPT.OBSERVATION.ONTOLOGY_ONTOBEE_URI
    ),
    constant_map(m="84413-4", out_col=CONCEPT.OBSERVATION.ONTOLOGY_CODE),
    keep_map(in_col="MaskID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="genotype", out_col=CONCEPT.OBSERVATION.NAME),
]
