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


class Genotype:
    class_name = "genotype"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        df = pd.DataFrame(records_list)
        df = df[df[CONCEPT.OBSERVATION.ONTOLOGY_CODE] == "84413-4"]

        return df.to_dict("records")

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        participant_id = not_none(record[CONCEPT.PARTICIPANT.ID])
        observation_name = not_none(record[CONCEPT.OBSERVATION.NAME])

        return {"identifier": f"{participant_id}-{observation_name}"}

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.PROJECT.ID]
        participant_id = record[CONCEPT.PARTICIPANT.ID]
        observation_name = record[CONCEPT.OBSERVATION.NAME]
        ontology_uri = record[CONCEPT.OBSERVATION.ONTOLOGY_ONTOBEE_URI]
        ontology_code = record[CONCEPT.OBSERVATION.ONTOLOGY_CODE]

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": f"{participant_id}-{observation_name}"}],
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
                    "text": "Test Results - Genotype",
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": ontology_uri,
                        "code": ontology_code,
                    }
                ],
            },
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
            "valueCodeableConcept": {"text": observation_name},
        }

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
