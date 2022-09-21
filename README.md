# CLOVoc App FHIR Ingest

The CLOVoc App FHIR Ingestor extracts records from harmonized tabular files, transforms to FHIR R4 resources, and loads them into a FHIR API.

## Quickstart

1. Make sure Python (>=3.7) is installed on your local machine or remote server where the ingestor is deployed.

2. Clone this repository:

```
$ git clone git@github.com:kids-first/clovoc-app-fhir-ingest.git
$ cd clovoc-app-fhir-ingest
```

3. Create a `.env` file in the root directory:

```
FHIR_COOKIE="FHIR-COOKIE"
FHIR_USERNAME="FHIR-USERNAME"
FHIR_PASSWORD="FHIR-PASSWORD"
```

4. Create and activate a virtual environment:

```
$ python3 -m venv venv
$ source venv/bin/activate
```

5. Install dependencies:

```
(venv) $ pip install --upgrade pip && pip install -e .
```

6. Get familiar with the CLI application:

```
(venv) kidsfirst -h

Usage: kidsfirst [OPTIONS] COMMAND [ARGS]...

  A CLI utility for ingesting tabular data into the Kids First ecosystem.

  This method does not need to be implemented. cli is the root group that all subcommands will implicitly be part of.

Options:
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Commands:
  ingest    Run the Kids First data ingest pipeline.
  new       Create a new ingest package based on the `kf_lib_data_ingest.templates.my_ingest_package` template.
  test      Run the Kids First data ingest pipeline with the --dry_run flag active.
  validate  Validate files and write validation reports to a subdirectory, `validation_results`, in the current working directory.
```

7. Run the following command:

```
(venv) kidsfirst ingest ./kf_ingest_packages/packages/CLOVoc \
  --no_validate \
  --app_settings ./target_api_plugins/clovoc_api_fhir_service.py \
  --target_url https://clovoc-api-fhir-service-dev.kf-strides.org \
  --stages etl \
  --use_async
```
