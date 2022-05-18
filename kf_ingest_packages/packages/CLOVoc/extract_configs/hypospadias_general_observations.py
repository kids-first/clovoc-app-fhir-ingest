"""
Extract config module for hypospadias - general observations.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map

source_data_url = "file://../data/hypospadias/hypospadiasGeneralObservations.tsv"


operations = [
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    value_map(
        in_col="Age at Observation Value",
        m=lambda x: int(x),
        out_col=CONCEPT.OBSERVATION.EVENT_AGE.VALUE,
    ),
    keep_map(
        in_col="Age at Observation Units", out_col=CONCEPT.OBSERVATION.EVENT_AGE.UNITS
    ),
    keep_map(in_col="Observation Name", out_col=CONCEPT.OBSERVATION.NAME),
    keep_map(in_col="Observation Ontology URI", out_col="OBSERVATION|ONTOLOGY_URI"),
    keep_map(in_col="Observation Code", out_col=CONCEPT.OBSERVATION.ONTOLOGY_CODE),
    value_map(
        in_col="Quantity Value",
        m=lambda x: int(x),
        out_col="OBSERVATION|QUANTITY|VALUE",
    ),
    keep_map(in_col="Quantity System", out_col="OBSERVATION|QUANTITY|SYSTEM"),
    keep_map(in_col="Quantity Units", out_col="OBSERVATION|QUANTITY|UNITS"),
    keep_map(in_col="Category", out_col=CONCEPT.OBSERVATION.CATEGORY),
    keep_map(in_col="Interpretation", out_col=CONCEPT.OBSERVATION.INTERPRETATION),
    keep_map(in_col="Body Site Name", out_col="OBSERVATION|BODY_SITE|NAME"),
    keep_map(
        in_col="Body Site Ontology URI", out_col="OBSERVATION|BODY_SITE|ONTOLOGY_URI"
    ),
    keep_map(in_col="Body Site Code", out_col="OBSERVATION|BODY_SITE|ONTOLOGY_CODE"),
]
