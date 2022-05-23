"""
Builds FHIR Patient resources (https://www.hl7.org/fhir/patient.html)
from rows of tabular participant data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# https://hl7.org/fhir/us/core/ValueSet-omb-race-category.html
omb_race_category = {
    constants.RACE.ASIAN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2028-9",
            "display": "Asian",
        },
    },
    constants.RACE.BLACK: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2054-5",
            "display": "Black or African American",
        },
    },
    constants.RACE.NATIVE_AMERICAN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "1002-5",
            "display": "American Indian or Alaska Native",
        },
    },
    constants.RACE.PACIFIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2076-8",
            "display": "Native Hawaiian or Other Pacific Islander",
        },
    },
    constants.RACE.WHITE: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2106-3",
            "display": "White",
        },
    },
    constants.COMMON.OTHER: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2131-1",
            "display": "Other Race",
        },
    },
    constants.COMMON.NOT_REPORTED: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "UNK",
            "display": "Unknown",
        },
    },
}

# https://hl7.org/fhir/us/core/ValueSet-omb-ethnicity-category.html
omb_ethnicity_category = {
    constants.ETHNICITY.HISPANIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2135-2",
            "display": "Hispanic or Latino",
        },
    },
    constants.ETHNICITY.NON_HISPANIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2186-5",
            "display": "Not Hispanic or Latino",
        },
    },
}

# http://hl7.org/fhir/R4/codesystem-administrative-gender.html
administrative_gender = {
    constants.GENDER.FEMALE: "female",
    constants.GENDER.MALE: "male",
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

deceased_boolean = {
    constants.OUTCOME.VITAL_STATUS.ALIVE: False,
    constants.OUTCOME.VITAL_STATUS.DEAD: True,
}


class Patient:
    class_name = "patient"
    api_path = "Patient"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.PARTICIPANT.ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]
        participant_id = record[CONCEPT.PARTICIPANT.ID]
        race = record.get(CONCEPT.PARTICIPANT.RACE)
        ethnicity = record.get(CONCEPT.PARTICIPANT.ETHNICITY)
        gender = record.get(CONCEPT.PARTICIPANT.GENDER)
        enrollment_age_value = record.get(CONCEPT.PARTICIPANT.ENROLLMENT_AGE.VALUE)
        enrollment_age_units = record.get(CONCEPT.PARTICIPANT.ENROLLMENT_AGE.UNITS)
        vital_status = record.get(CONCEPT.OUTCOME.VITAL_STATUS)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": participant_id}],
        }

        # US Core Race
        us_core_race = {}
        if race:
            us_core_race.update(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [{"url": "text", "valueString": race}],
                }
            )
            if omb_race_category.get(race):
                us_core_race["extension"].append(omb_race_category[race])
        if us_core_race:
            entity.setdefault("extension", []).append(us_core_race)

        # US Core Ethnicity
        us_core_ethnicity = {}
        if ethnicity:
            us_core_ethnicity.update(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [{"url": "text", "valueString": ethnicity}],
                }
            )
            if omb_ethnicity_category.get(ethnicity):
                us_core_ethnicity["extension"].append(omb_ethnicity_category[ethnicity])
        if us_core_ethnicity:
            entity.setdefault("extension", []).append(us_core_ethnicity)

        # gender
        if administrative_gender.get(gender):
            entity["gender"] = administrative_gender[gender]

        # birthDate
        try:
            entity["_birthDate"] = {
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
                                    "value": int(enrollment_age_value),
                                    "unit": age_units_to_unit[enrollment_age_units],
                                    "system": "http://unitsofmeasure.org",
                                    "code": age_units_to_code[enrollment_age_units],
                                },
                            },
                        ],
                        "url": "http://hl7.org/fhir/StructureDefinition/relative-date",
                    }
                ]
            }
        except:
            pass

        # deceasedBoolean
        if deceased_boolean.get(vital_status):
            entity["deceasedBoolean"] = deceased_boolean[vital_status]

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
