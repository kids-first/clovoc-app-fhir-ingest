"""
Transform module

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""

import pandas as pd

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates, outer_merge
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    # Particiapnt details
    participant_details = merge_wo_duplicates(
        mapped_df_dict["screening_form.py"],
        mapped_df_dict["enrollment_form.py"],
        how="inner",
        on=CONCEPT.PARTICIPANT.ID,
    )

    # Lab test results
    test_results = pd.concat(
        [
            mapped_df_dict["antibodies.py"],
            mapped_df_dict["chemistry.py"],
            mapped_df_dict["genotype.py"],
        ],
        ignore_index=True,
    )
    merged_df = outer_merge(
        participant_details,
        test_results,
        with_merge_detail_dfs=False,
        on=CONCEPT.PARTICIPANT.ID,
    )

    return {DEFAULT_KEY: merged_df}
