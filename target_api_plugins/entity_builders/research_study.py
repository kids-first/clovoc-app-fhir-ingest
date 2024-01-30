"""
Builds FHIR ResearchStudy resources (https://www.hl7.org/fhir/researchstudy.html)
from rows of tabular study metadata.
"""
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/research-study-status
status = "completed"

study_id_to_title_dict = {
    "phs001442": "The Environmental Determinants of Diabetes in the Young Study (TEDDY)"
}


class ResearchStudy:
    class_name = "research_study"
    api_path = "ResearchStudy"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.PROJECT.ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.PROJECT.ID]

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": study_id}],
            "title": study_id_to_title_dict[study_id],
            "status": status,
        }

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
