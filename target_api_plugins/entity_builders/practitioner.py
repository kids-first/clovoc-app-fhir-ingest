"""
Builds FHIR Practitioner resources (https://www.hl7.org/fhir/practitioner.html)
from rows of tabular practitioner metadata.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids


class Practitioner:
    class_name = "practitioner"
    api_path = "Practitioner"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record["PRACTITIONER|NAME"])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        name = record["PRACTITIONER|NAME"]
        phone = record.get("PRACTITIONER|TELECOM|PHONE")
        email = record.get("PRACTITIONER|TELECOM|EMAIL")
        address = record.get("PRACTITIONER|ADDRESS")

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": name}],
            "active": True,
            "name": [{"text": name}],
        }

        # telecom
        telecom = []
        if phone:
            telecom.append(
                {
                    "system": "phone",
                    "value": phone,
                }
            )
        if email:
            telecom.append(
                {
                    "system": "email",
                    "value": email,
                }
            )
        if telecom:
            entity["telecom"] = telecom

        # address
        if address:
            entity.setdefault("address", []).append({"text": address})

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
