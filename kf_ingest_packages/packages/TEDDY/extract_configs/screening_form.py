"""
Extract config module for TEDDY - SCREENING_FROM.CSV.

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/tutorial/extract.html for
information on writing extract config files.
"""

from kf_lib_data_ingest.etl.extract.operations import keep_map, row_map, value_map
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common import constants

source_data_url = "file://../data/SCREENING_FORM.csv"

race_column_to_constant_dict = {
    "RACE_ASIAN": constants.RACE.ASIAN,
    "RACE_BLACKORAFRICANAMERICAN": constants.RACE.BLACK,
    "RACE_NATIVEAMERICANALASKANNATI": constants.RACE.NATIVE_AMERICAN,
    "RACE_NATIVEHAWAIIANOROTHERPACI": constants.RACE.PACIFIC,
    "RACE_UNKNOWNORNOTREPORTED": constants.COMMON.UNKNOWN,
    "RACE_WHITE": constants.RACE.WHITE,
}


def get_race(row):
    race_list = [
        race_column_to_constant_dict[column]
        for column in race_column_to_constant_dict
        if row.get(column)
    ]
    return (
        next(iter(race_list))
        if len(race_list) == 1
        else constants.RACE.MULTIPLE
        if len(race_list) > 1
        else None
    )


operations = [
    keep_map(in_col="MaskID", out_col=CONCEPT.PARTICIPANT.ID),
    row_map(m=get_race, out_col=CONCEPT.PARTICIPANT.RACE),
    value_map(
        m={
            "Yes": constants.ETHNICITY.HISPANIC,
            "No": constants.ETHNICITY.NON_HISPANIC,
            "Unknown or not reported": constants.COMMON.UNKNOWN,
        },
        in_col="ETHNICITY",
        out_col=CONCEPT.PARTICIPANT.ETHNICITY,
    ),
    keep_map(in_col="SEX", out_col=CONCEPT.PARTICIPANT.GENDER),
]
