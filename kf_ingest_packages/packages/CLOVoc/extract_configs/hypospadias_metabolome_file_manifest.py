"""
Extract config module for hypospadias - metabolome file manifest.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.etl.extract.operations import keep_map, value_map

source_data_url = "file://../data/hypospadias/MetabolomeFileManifest.tsv"


operations = [
    keep_map(in_col="Study ID", out_col=CONCEPT.STUDY.ID),
    keep_map(in_col="Participant ID", out_col=CONCEPT.PARTICIPANT.ID),
    keep_map(in_col="Specimen ID", out_col=CONCEPT.BIOSPECIMEN.ID),
    keep_map(in_col="Specimen ID", out_col=CONCEPT.GENOMIC_FILE.ID),
    keep_map(in_col="Analyte Type", out_col=CONCEPT.BIOSPECIMEN.ANALYTE),
    value_map(
        in_col="Metabolite Data URL",
        m=lambda x: [x],
        out_col=CONCEPT.GENOMIC_FILE.URL_LIST,
    ),
    keep_map(in_col="Lab", out_col="PRACTITIONER|ADDRESS"),
    keep_map(in_col="Contact Person", out_col="PRACTITIONER|NAME"),
    keep_map(in_col="Email", out_col="PRACTITIONER|TELECOM|EMAIL"),
    keep_map(in_col="Phone", out_col="PRACTITIONER|TELECOM|PHONE"),
]
