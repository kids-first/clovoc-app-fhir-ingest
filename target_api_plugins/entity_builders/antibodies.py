"""
Builds FHIR Observation resources (https://www.hl7.org/fhir/observation.html)
from rows of tabular antibodies data.
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

# http://hl7.org/fhir/ValueSet/observation-interpretation
interpretation_coding = {
    "Positive": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
        "code": "POS",
        "display": "Positive",
    }
}


class Antibodies:
    class_name = "antibodies"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        df = pd.DataFrame(records_list)
        df = df[df[CONCEPT.OBSERVATION.NAME].isin(["GAD", "IA2A", "MIAA"])]
        transformed_records = []
        for names, group in df.groupby(
            by=[
                CONCEPT.PARTICIPANT.ID,
                CONCEPT.OBSERVATION.NAME,
                CONCEPT.OBSERVATION.ONTOLOGY_CODE,
                CONCEPT.OBSERVATION.EVENT_AGE.VALUE,
                CONCEPT.OBSERVATION.EVENT_AGE.UNITS,
                CONCEPT.OBSERVATION.INTERPRETATION,
            ]
        ):
            record = {
                CONCEPT.PARTICIPANT.ID: names[0],
                CONCEPT.OBSERVATION.NAME: names[1],
                CONCEPT.OBSERVATION.ONTOLOGY_CODE: names[2],
                CONCEPT.OBSERVATION.EVENT_AGE.VALUE: names[3],
                CONCEPT.OBSERVATION.EVENT_AGE.UNITS: names[4],
                CONCEPT.OBSERVATION.INTERPRETATION: names[5],
            }
            component_list = []
            for _, row in group.iterrows():
                name, value, units = row.get(
                    [
                        "OBSERVATION|COMPONENT|NAME",
                        "OBSERVATION|QUANTITY|VALUE",
                        "OBSERVATION|QUANTITY|UNITS",
                    ]
                )
                component_list.append(
                    {
                        "OBSERVATION|COMPONENT|NAME": name,
                        "OBSERVATION|QUANTITY|VALUE": value,
                        "OBSERVATION|QUANTITY|UNITS": units,
                    }
                )
            record["OBSERVATION|COMPONENT"] = component_list
            transformed_records.append(record)

        return transformed_records

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
        ontology_code = record[CONCEPT.OBSERVATION.ONTOLOGY_CODE]
        event_age_value = record[CONCEPT.OBSERVATION.EVENT_AGE.VALUE]
        event_age_units = record[CONCEPT.OBSERVATION.EVENT_AGE.UNITS]
        interpretation = record[CONCEPT.OBSERVATION.INTERPRETATION]
        component_list = record["OBSERVATION|COMPONENT"]

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
                    "text": "Test Results - Antibodies",
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
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
            "interpretation": [
                {
                    "coding": [interpretation_coding[interpretation]],
                    "text": interpretation,
                }
            ],
        }

        for component in component_list:
            entity.setdefault("component", []).append(
                {
                    "code": {"text": component["OBSERVATION|COMPONENT|NAME"]},
                    "valueQuantity": {
                        "value": float(component["OBSERVATION|QUANTITY|VALUE"]),
                        "system": "http://unitsofmeasure.org",
                        "code": component["OBSERVATION|QUANTITY|UNITS"],
                    },
                }
            )

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
