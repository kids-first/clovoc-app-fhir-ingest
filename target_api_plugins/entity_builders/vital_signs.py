"""
Builds FHIR Patient resources (https://www.hl7.org/fhir/patient.html)
from rows of tabular vital signs data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Patient
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/observation-status
status_code = "final"

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

# http://unitsofmeasure.org
quantity_code_to_unit = {
    "Cel": "degree Celsius",
    "cm": "centimeter",
    "kg": "kilogram",
    "kg/m2": "kilogram per square meter",
    "[degF]": "degree Fahrenheit",
}

# http://hl7.org/fhir/ValueSet/observation-interpretation
interpretation_coding = {
    "High": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
        "code": "H",
        "display": "High",
    },
    "Low": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
        "code": "L",
        "display": "Low",
    },
    "Normal": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
        "code": "N",
        "display": "Normal",
    },
}

missing_data_values = {
    constants.COMMON.NOT_REPORTED,
    constants.COMMON.OTHER,
}


class VitalSigns:
    class_name = "vital_signs"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        participant_id = not_none(record[CONCEPT.PARTICIPANT.ID])
        category = not_none(record[CONCEPT.OBSERVATION.CATEGORY])
        name = not_none(record[CONCEPT.OBSERVATION.NAME])

        return {"identifier": f"{participant_id}-{category}-{name}"}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        participant_id = record[CONCEPT.PARTICIPANT.ID]
        category = record[CONCEPT.OBSERVATION.CATEGORY]
        name = record[CONCEPT.OBSERVATION.NAME]
        ontology_uri = record.get("OBSERVATION|ONTOLOGY_URI")
        ontology_code = record.get(CONCEPT.OBSERVATION.ONTOLOGY_CODE)
        event_age_value = record.get(CONCEPT.OBSERVATION.EVENT_AGE.VALUE)
        event_age_units = record.get(CONCEPT.OBSERVATION.EVENT_AGE.UNITS)
        quantity_value = record.get("OBSERVATION|QUANTITY|VALUE")
        quantity_ontology_uri = record.get("OBSERVATION|QUANTITY|ONTOLOGY_URI")
        quantity_ontology_code = record.get("OBSERVATION|QUANTITY|ONTOLOGY_CODE")
        interpretation = record.get(CONCEPT.OBSERVATION.INTERPRETATION)
        body_site_name = record.get("OBSERVATION|BODY_SITE|NAME")
        body_site_ontology_uri = record.get("OBSERVATION|BODY_SITE|ONTOLOGY_URI")
        body_site_ontology_code = record.get("OBSERVATION|BODY_SITE|ONTOLOGY_CODE")

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": f"{participant_id}-{category}-{name}"}],
            "status": status_code,
            "categoty": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "vital-signs",
                            "display": "Vital Signs",
                        }
                    ],
                    "text": category,
                }
            ],
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

        # effectiveDataTime
        try:
            entity["_effectiveDataTime"] = {
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

        # valueQuantity
        if (
            quantity_value
            and quantity_value not in missing_data_values
            and quantity_ontology_uri
            and quantity_ontology_code
        ):
            try:
                quantity_value = int(quantity_value)
            except:
                quantity_value = float(quantity_value)

            entity["valueQuantity"] = {
                "value": quantity_value,
                "unit": quantity_code_to_unit[quantity_ontology_code],
                "system": quantity_ontology_uri,
                "code": quantity_ontology_code,
            }

        # interpretation
        interpretation_dict = {}
        if interpretation and interpretation not in missing_data_values:
            interpretation_dict["text"] = interpretation
            if interpretation_coding.get(interpretation):
                interpretation_dict.setdefault("coding", []).append(
                    interpretation_coding[interpretation]
                )
        if interpretation_dict:
            entity.setdefault("interpretation", []).append(interpretation_dict)

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
            entity["bodySite"] = body_site

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
