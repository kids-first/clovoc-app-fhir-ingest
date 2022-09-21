"""
Transform module.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing transform_function.
"""

import pandas as pd

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.config import DEFAULT_KEY


def transform_function(mapped_df_dict):
    # Particiapnt details
    participant_details = pd.concat(
        [
            mapped_df_dict["hypospadias_participant_details.py"],
            mapped_df_dict["vur_participant_details.py"],
            mapped_df_dict["kf_participant_details.py"],
        ],
        ignore_index=True,
    )

    # Participant phenotypes
    participant_phenotypes = pd.concat(
        [
            mapped_df_dict["hypospadias_participant_phenotypes.py"],
            mapped_df_dict["vur_participant_phenotypes.py"],
            mapped_df_dict["kf_participant_phenotypes.py"],
        ],
        ignore_index=True,
    )

    # General observations
    general_observations = pd.concat(
        [
            mapped_df_dict["hypospadias_general_observations.py"],
            mapped_df_dict["vur_general_observations.py"],
        ],
        ignore_index=True,
    )

    # Biospecimen collection manifest
    biospecimen_collection_manifest = pd.concat(
        [
            mapped_df_dict["hypospadias_biospecimen_collection_manifest.py"],
            mapped_df_dict["vur_biospecimen_collection_manifest.py"],
            mapped_df_dict["kf_biospecimen_collection_manifest.py"],
        ],
        ignore_index=True,
    )

    # Metabolome file manifest
    metabolome_file_manifest = pd.concat(
        [
            mapped_df_dict["hypospadias_metabolome_file_manifest.py"],
            mapped_df_dict["vur_metabolome_file_manifest.py"],
        ],
        ignore_index=True,
    )

    # Outer-merge particiapnt details and participant phenotypes
    df = outer_merge(
        participant_details,
        participant_phenotypes,
        on=CONCEPT.PARTICIPANT.ID,
        with_merge_detail_dfs=False,
    )

    # Outer-merge the above and general observations
    df = outer_merge(
        df, general_observations, on=CONCEPT.PARTICIPANT.ID, with_merge_detail_dfs=False
    )

    # Outer-merge the above and biospecimen collection manifest
    df = outer_merge(
        df,
        biospecimen_collection_manifest,
        on=CONCEPT.PARTICIPANT.ID,
        with_merge_detail_dfs=False,
    )

    # Outer-merge the above and matabolome file manifest
    df = outer_merge(
        df,
        metabolome_file_manifest,
        on=[
            CONCEPT.PARTICIPANT.ID,
            CONCEPT.BIOSPECIMEN.ID,
        ],
        with_merge_detail_dfs=False,
    )

    return {DEFAULT_KEY: df}
