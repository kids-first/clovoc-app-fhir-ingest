"""
Builds FHIR Observation resources (https://www.hl7.org/fhir/observation.html)
from rows of tabular chemistry data.
"""
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/observation-status
status_code = "final"

# http://unitsofmeasure.org
age_units_to_unit = {constants.AGE.UNITS.MONTHS: "month"}

# http://unitsofmeasure.org
age_units_to_code = {constants.AGE.UNITS.MONTHS: "mo"}


class Chemistry:
    class_name = "chemistry"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        df = pd.DataFrame(records_list)
        df = df[
            (df[CONCEPT.OBSERVATION.NAME] == "HbA1c")
            | (df[CONCEPT.OBSERVATION.NAME].str.startswith("OGTTPEP"))
            | (df[CONCEPT.OBSERVATION.NAME].str.startswith("GLU"))
            | (df[CONCEPT.OBSERVATION.NAME].str.startswith("OGTTINS"))
            | (df[CONCEPT.OBSERVATION.NAME].str.startswith("OGTTGLU"))
        ]

        return df.to_dict("records")

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        participant_id = not_none(record[CONCEPT.PARTICIPANT.ID])
        observation_name = not_none(record[CONCEPT.OBSERVATION.NAME])
        event_age_value = not_none(record[CONCEPT.OBSERVATION.EVENT_AGE.VALUE])
        event_age_units = not_none(record[CONCEPT.OBSERVATION.EVENT_AGE.UNITS])

        return {
            "identifier": "-".join(
                [
                    participant_id,
                    observation_name,
                    event_age_value,
                    event_age_units,
                ]
            )
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.PROJECT.ID]
        participant_id = record[CONCEPT.PARTICIPANT.ID]
        observation_name = record[CONCEPT.OBSERVATION.NAME]
        ontology_uri = record[CONCEPT.OBSERVATION.ONTOLOGY_ONTOBEE_URI]
        ontology_code = record[CONCEPT.OBSERVATION.ONTOLOGY_CODE]
        event_age_value = record[CONCEPT.OBSERVATION.EVENT_AGE.VALUE]
        event_age_units = record[CONCEPT.OBSERVATION.EVENT_AGE.UNITS]
        value = record["OBSERVATION|QUANTITY|VALUE"]
        units = record["OBSERVATION|QUANTITY|UNITS"]

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [
                {
                    "value": "-".join(
                        [
                            participant_id,
                            observation_name,
                            event_age_value,
                            event_age_units,
                        ]
                    )
                }
            ],
            "status": status_code,
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "laboratory",
                            "display": "Laboratory",
                        }
                    ],
                    "text": "Test Results - Chemistry",
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": ontology_uri,
                        "code": ontology_code,
                    }
                ],
                "text": observation_name,
            },
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
            "_effectiveDateTime": {
                "extension": [
                    {
                        "extension": [
                            {
                                "url": "target",
                                "valueReference": {
                                    "reference": "/".join(
                                        [
                                            Patient.api_path,
                                            not_none(
                                                get_target_id_from_record(
                                                    Patient, record
                                                )
                                            ),
                                        ]
                                    )
                                },
                            },
                            {
                                "url": "targetPath",
                                "valueString": "birthDate",
                            },
                            {
                                "url": "relationship",
                                "valueCode": "after",
                            },
                            {
                                "url": "offset",
                                "valueDuration": {
                                    "value": int(event_age_value),
                                    "unit": age_units_to_unit[event_age_units],
                                    "system": "http://unitsofmeasure.org",
                                    "code": age_units_to_code[event_age_units],
                                },
                            },
                        ],
                        "url": "http://hl7.org/fhir/StructureDefinition/cqf-relativeDateTime",
                    }
                ]
            },
        }

        # valueQuantity
        value_quantity = {"system": "http://unitsofmeasure.org"}
        value_quantity["value"] = float(
            value if not value.startswith("<") else value.lsrip("<")
        )
        comparator = "<" if value.startswith("<") else None
        if comparator is not None:
            value_quantity["comparator"] = comparator
        unit = units if units == "percent" else None
        if unit is not None:
            value_quantity["unit"] = unit
        value_quantity["code"] = units if units != "percent" else "%"
        entity["valueQuantity"] = value_quantity

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
