"""
Builds FHIR Patient resources (https://www.hl7.org/fhir/patient.html)
from rows of tabular metabolomic data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/document-reference-status
status_code = "current"

# http://hl7.org/fhir/ValueSet/composition-status
doc_status_code = "final"

# https://includedcc.org/fhir/code-systems/data_categories
data_cateogry_coding = {
    "Metabolome": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Metabolomic",
        "display": "Metabolomic",
    },
}


class DocumentReference:
    class_name = "document_reference"
    api_path = "DocumentReference"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.GENOMIC_FILE.ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        file_id = record[CONCEPT.GENOMIC_FILE.ID]
        analyte = record.get(CONCEPT.BIOSPECIMEN.ANALYTE)
        url_list = record.get(CONCEPT.GENOMIC_FILE.URL_LIST)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": file_id}],
            "status": status_code,
            "docStatus": doc_status_code,
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
        }

        # category
        category = {}
        if analyte:
            category["text"] = analyte
            if data_cateogry_coding.get(analyte):
                category.setdefault("coding", []).append(data_cateogry_coding[analyte])
        if category:
            entity.setdefault("category", []).append(category)

        # content
        if url_list:
            entity.setdefault("content", []).append(
                {"attachment": {"url": eval(url_list)[0]}}
            )

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
