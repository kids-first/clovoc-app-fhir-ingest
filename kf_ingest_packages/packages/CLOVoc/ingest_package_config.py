""" Ingest Package Config """

# The list of entities that will be loaded into the target service. These
# should be class_name values of your target API config's target entity
# classes.
target_service_entities = [
    "practitioner",
    "patient",
    "group",
    "research_study",
    "research_subject",
    "phenotype",
    "vital_signs",
    "specimen",
    "document_reference",
]

# All paths are relative to the directory this file is in
extract_config_dir = "extract_configs"

transform_function_path = "transform_module.py"

project = "CLOVoc"
