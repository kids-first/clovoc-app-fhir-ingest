"""
Builds FHIR ResearchStudy resources (https://www.hl7.org/fhir/researchstudy.html)
from rows of tabular study metadata.
"""
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from target_api_plugins.entity_builders import Group
from target_api_plugins.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/research-study-status
status = "active"

study_id_to_title = {
    "ST001306": "Biomolecular analyses of hypospadias according to severity",
    "ST001649": "Urinary microbiota and metabolome in pediatric vesicoureteral reflux and scarring",
}


class ResearchStudy:
    class_name = "research_study"
    api_path = "ResearchStudy"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        return [
            {
                CONCEPT.STUDY.ID: study_id,
                "GROUP|NAME": group.get("GROUP|NAME").unique(),
                CONCEPT.PHENOTYPE: group[
                    [
                        CONCEPT.PHENOTYPE.NAME,
                        "PHENOTYPE|ONTOLOGY_URI",
                        "PHENOTYPE|ONTOLOGY_CODE",
                    ]
                ]
                .drop_duplicates()
                .to_dict(orient="records"),
            }
            for study_id, group in pd.DataFrame(records_list).groupby(CONCEPT.STUDY.ID)
        ]

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.STUDY.ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.ID]

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [{"value": study_id}],
            "title": study_id_to_title[study_id],
            "status": status,
        }

        # condition
        condition = []
        for phenotype in record.get(CONCEPT.PHENOTYPE, []):
            codeable_concept = {}
            if phenotype.get(CONCEPT.PHENOTYPE.NAME):
                codeable_concept["text"] = phenotype[CONCEPT.PHENOTYPE.NAME]
            if phenotype.get("PHENOTYPE|ONTOLOGY_URI") and phenotype.get(
                "PHENOTYPE|ONTOLOGY_CODE"
            ):
                codeable_concept.setdefault("coding", []).append(
                    {
                        "system": phenotype["PHENOTYPE|ONTOLOGY_URI"],
                        "code": phenotype["PHENOTYPE|ONTOLOGY_CODE"],
                    }
                )
            if codeable_concept:
                condition.append(codeable_concept)
        if condition:
            entity["condition"] = condition

        # enrollment
        enrollment = []
        for group_name in record.get("GROUP|NAME", []):
            group_id = not_none(
                get_target_id_from_record(
                    Group,
                    {
                        CONCEPT.STUDY.ID: study_id,
                        "GROUP|NAME": group_name,
                    },
                )
            )
            enrollment.append({"reference": f"{Group.api_path}/{group_id}"})
        if enrollment:
            entity["enrollment"] = enrollment

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
