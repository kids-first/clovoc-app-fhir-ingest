"""
Builds FHIR ResearchStudy resources (https://www.hl7.org/fhir/researchstudy.html)
from rows of tabular group metadata.
"""
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids


class Group:
    class_name = "group"
    api_path = "Group"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        return [
            {
                CONCEPT.STUDY.ID: study_id,
                "GROUP|NAME": name,
                CONCEPT.PARTICIPANT.ID: group.get(CONCEPT.PARTICIPANT.ID).unique(),
            }
            for (study_id, name), group in pd.DataFrame(records_list).groupby(
                [
                    CONCEPT.STUDY.ID,
                    "GROUP|NAME",
                ]
            )
        ]

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        study_id = not_none(record[CONCEPT.STUDY.ID])
        name = not_none(record["GROUP|NAME"])

        return {"identifier": f"{study_id}-{name}"}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        name = record["GROUP|NAME"]

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": f"{study_id}-{name}"}],
            "type": "person",
            "actual": True,
            "name": name,
        }

        # quantity; member
        member = []
        for participant_id in record.get(CONCEPT.PARTICIPANT.ID, []):
            patient_id = not_none(
                get_target_id_from_record(
                    Patient, {CONCEPT.PARTICIPANT.ID: participant_id}
                )
            )
            member.append(
                {
                    "entity": {"reference": f"{Patient.api_path}/{patient_id}"},
                    "inactive": False,
                }
            )
        if member:
            entity["quantity"] = len(member)
            entity["member"] = member

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
