"""
Builds FHIR Patient resources (https://www.hl7.org/fhir/patient.html)
from rows of tabular biospecimen data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/specimen-status
status_code = "unavailable"

# http://unitsofmeasure.org
age_units_to_unit = {
    constants.AGE.UNITS.DAYS: "day",
    constants.AGE.UNITS.MONTHS: "month",
    constants.AGE.UNITS.YEARS: "year",
}

# http://unitsofmeasure.org
age_units_to_code = {
    constants.AGE.UNITS.DAYS: "d",
    constants.AGE.UNITS.MONTHS: "mo",
    constants.AGE.UNITS.YEARS: "a",
}


class Specimen:
    class_name = "specimen"
    api_path = "Specimen"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.BIOSPECIMEN.ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        biospecimen_id = record[CONCEPT.BIOSPECIMEN.ID]
        type_name = record.get("BIOSPECIMEN|TYPE|NAME")
        type_ontology_uri = record.get("BIOSPECIMEN|TYPE|ONTOLOGY_URI")
        type_ontology_code = record.get("BIOSPECIMEN|TYPE|ONTOLOGY_CODE")
        event_age_value = record.get(CONCEPT.BIOSPECIMEN.EVENT_AGE.VALUE)
        event_age_units = record.get(CONCEPT.BIOSPECIMEN.EVENT_AGE.UNITS)
        sample_procurement = record.get(CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT)
        body_site_name = record.get("BIOSPECIMEN|BODY_SITE|NAME")
        body_site_ontology_uri = record.get("BIOSPECIMEN|BODY_SITE|ONTOLOGY_URI")
        body_site_ontology_code = record.get("BIOSPECIMEN|BODY_SITE|ONTOLOGY_CODE")

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": biospecimen_id}],
            "status": status_code,
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
        }

        # type
        specimen_type = {}
        if type_name:
            specimen_type["text"] = type_name
        if type_ontology_uri and type_ontology_code:
            specimen_type.setdefault("coding", []).append(
                {
                    "system": type_ontology_uri,
                    "code": type_ontology_code,
                }
            )
        if specimen_type:
            entity["type"] = specimen_type

        # collection
        collection = {}

        # collectedDateTime
        try:
            collection["_collectedDateTime"] = {
                "extension": [
                    {
                        "extension": [
                            {
                                "url": "event",
                                "valueCodeableConcept": {
                                    "coding": [
                                        {
                                            "system": "http://snomed.info/sct",
                                            "code": "3950001",
                                            "display": "Birth",
                                        }
                                    ]
                                },
                            },
                            {"url": "relationship", "valueCode": "after"},
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
                        "url": "http://hl7.org/fhir/StructureDefinition/relative-date",
                    }
                ]
            }
        except:
            pass

        # method
        if sample_procurement:
            collection["method"] = {"text": sample_procurement}

        # bodySite
        body_site = {}
        if body_site_name:
            body_site["text"] = body_site_name
        if body_site_ontology_uri and body_site_ontology_code:
            body_site.setdefault("coding", []).append(
                {
                    "system": body_site_ontology_uri,
                    "code": body_site_ontology_code,
                }
            )
        if body_site:
            collection["bodySite"] = body_site

        if collection:
            entity["collection"] = collection

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
