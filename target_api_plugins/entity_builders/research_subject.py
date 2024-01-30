"""
Builds FHIR ResearchSubject resources (https://www.hl7.org/fhir/researchsubject.html) 
from rows of tabular participant data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import ResearchStudy, Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/research-subject-status
status = "off-study"


class ResearchSubject:
    class_name = "research_subject"
    api_path = "ResearchSubject"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        research_study_id = not_none(get_target_id_from_record(ResearchStudy, record))
        patient_id = not_none(get_target_id_from_record(Patient, record))
        return {
            "study": f"{ResearchStudy.api_path}/{research_study_id}",
            "individual": f"{Patient.api_path}/{patient_id}",
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.PROJECT.ID]
        participant_id = record[CONCEPT.PARTICIPANT.ID]
        key_components = cls.get_key_components(record, get_target_id_from_record)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": participant_id}],
            "status": status,
            "study": {"reference": key_components["study"]},
            "individual": {"reference": key_components["individual"]},
        }

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
