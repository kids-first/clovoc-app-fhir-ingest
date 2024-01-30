""" Ingest Package Config """

# The list of entities that will be loaded into the target service. These
# should be class_name values of your target API config's target entity
# classes.
target_service_entities = [
    # "patient",
    # "research_study",
    # "research_subject",
    # "antibodies",
    # "chemistry",
    "genotype",
]

# All paths are relative to the directory this file is in
extract_config_dir = "extract_configs"

transform_function_path = "transform_module.py"

# TODO - Replace this with your own unique identifier for the project. This
# will become CONCEPT.PROJECT.ID during the Load stage.
project = "phs001442"
