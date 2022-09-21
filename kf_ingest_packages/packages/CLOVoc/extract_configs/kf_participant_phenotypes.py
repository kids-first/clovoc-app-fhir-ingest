"""
Extract config module for kids first - participant phenotypes.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map

source_data_url = "file://../data/kids-first/ParticipantPhenotypes.tsv"


operations = [
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="Condition Name", out_col=CONCEPT.PHENOTYPE.NAME),
    keep_map(in_col="Condition Ontology URI", out_col="PHENOTYPE|ONTOLOGY_URI"),
    keep_map(in_col="Condition Code", out_col="PHENOTYPE|ONTOLOGY_CODE"),
    keep_map(in_col="Verification Status", out_col=CONCEPT.PHENOTYPE.VERIFICATION),
]
