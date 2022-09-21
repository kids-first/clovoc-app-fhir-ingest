"""
Builds FHIR Patient resources (https://www.hl7.org/fhir/patient.html)
from rows of tabular phenotype data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/condition-ver-status
verification_status_coding = {
    constants.PHENOTYPE.OBSERVED.NO: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "refuted",
        "display": "Refuted",
    },
    constants.PHENOTYPE.OBSERVED.YES: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "confirmed",
        "display": "Confirmed",
    },
    constants.COMMON.UNKNOWN: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "unconfirmed",
        "display": "Unconfirmed",
    },
}

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

missing_data_values = {constants.COMMON.NOT_APPLICABLE}


class Phenotype:
    class_name = "phenotype"
    api_path = "Condition"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        participant_id = not_none(record[CONCEPT.PARTICIPANT.ID])
        name = not_none(record[CONCEPT.PHENOTYPE.NAME])
        verification = not_none(record[CONCEPT.PHENOTYPE.VERIFICATION])

        return {"identifier": f"{participant_id}-{name}-{verification}"}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        participant_id = record[CONCEPT.PARTICIPANT.ID]
        verification = record[CONCEPT.PHENOTYPE.VERIFICATION]
        name = record[CONCEPT.PHENOTYPE.NAME]
        ontology_uri = record.get("PHENOTYPE|ONTOLOGY_URI")
        ontology_code = record.get("PHENOTYPE|ONTOLOGY_CODE")
        body_site_name = record.get("PHENOTYPE|BODY_SITE|NAME")
        body_site_ontology_uri = record.get("PHENOTYPE|BODY_SITE|ONTOLOGY_URI")
        body_site_ontology_code = record.get("PHENOTYPE|BODY_SITE|ONTOLOGY_CODE")
        event_age_value = record.get(CONCEPT.PHENOTYPE.EVENT_AGE.VALUE)
        event_age_units = record.get(CONCEPT.PHENOTYPE.EVENT_AGE.UNITS)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": f"{participant_id}-{name}-{verification}"}],
            "verificationStatus": {
                "coding": [verification_status_coding[verification]],
                "text": verification,
            },
            "code": {"text": name},
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
        }

        # code
        if ontology_uri and ontology_code:
            entity["code"].setdefault("coding", []).append(
                {
                    "system": ontology_uri,
                    "code": ontology_code,
                }
            )

        # bodySite
        body_site = {}
        if body_site_name and body_site_name not in missing_data_values:
            body_site["text"] = body_site_name
        if (
            body_site_ontology_uri
            and body_site_ontology_uri not in missing_data_values
            and body_site_ontology_code
            and body_site_ontology_code not in missing_data_values
        ):
            body_site.setdefault("coding", []).append(
                {
                    "system": body_site_ontology_uri,
                    "code": body_site_ontology_code,
                }
            )
        if body_site:
            entity.setdefault("bodySite", []).append(body_site)

        # onsetDateTime
        try:
            entity["onsetDateTime"] = {
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

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
