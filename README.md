# classifier-pipeline

A software suite that enables remote extraction, transformation and loading of data.

This repository is geared heavily towards drawing articles from PubMed, identifying scientific articles containing information about biological pathways, and loading the records into a data store.

## Requirements

- [Python (version >=3.8<3.10)](https://www.python.org/)
- [Poetry (version >1.0.0)](https://python-poetry.org/)
- [Docker (version 20.10.14) and Docker Compose (version 2.5.1)](https://www.docker.com/)
  - We use Docker to create a [RethinkDB (v2.3.6)](https://rethinkdb.com/) instance for loading data.
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (Optional)
  - For creating virtual environments. Any other solution will work.
- Graphics Processing Unit (GPU) (Optional)
  - The pipeline classifier can be sped up an order of magnitude by running on a system with a GPU. We have been using a system running Ubuntu 18.04.5 LTS, Intel(R) Xeon(R) CPU E5-2687W, 24 Core with an NVIDIA GP102 [TITAN Xp] GPU.

## Usage

Create a conda environment, here named `pipeline`:

```bash
$ conda create --name pipeline python=3.8 --yes
$ conda activate pipeline
```

Download the remote:

```bash
$ git clone https://github.com/jvwong/classifier-pipeline
$ cd classifier-pipeline
```

Install the dependencies:

```bash
$ poetry install
```

Launch a pipeline to process daily updates from PubMed and dump the RethinkDB database:

```bash
$ ./scripts/cron/install.sh
```

## Elements of the 'Pipeline'

### The pipeline

The `scripts` directory contains python files that chain functions in `classifier_pipeline` to:
- read in data from
  - csv, via stdin (`csv2dict_reader`)
  - [daily PubMed updates](https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/) (`updatefiles_extractor`)
- retrieve records/files from PubMed (`pubmed_transformer`)
- apply various filters on the individual records (`citation_pubtype_filter`, `citation_date_filter`)
- apply a deep-learning classifier to text fields (`classification_transformer`)
- loads the formatted data into a RethinkDB instance (`db_loader`)

### Launchers

- Pipelines are launched through bash scripts that retrieve PubMed article records in two ways:
    - `./scripts/cron/cron.sh`: retrieves via the [FTP file server](https://www.nlm.nih.gov/databases/download/pubmed_medline.html) all new content
    - `./scripts/csv/pmids.sh`: retrieve using the [NCBI E-Utilities](https://www.ncbi.nlm.nih.gov/books/NBK25499/) given a set of PubMed IDs
- Variables
    - `DATA_DIR` root directory where your data files exist
    - `DATA_FILE` name of the csv file in your `DATA_DIR`
    - `ARG_IDCOLUMN` the csv header column name containing either
        - a list of update files to extract (`dailyupdates.sh`)
        - a list of PubMed IDs to extract (`pmids.sh`)
    - `JOB_NAME` the name of this pipeline job
    - `CONDA_ENV` should be the environment name you declared in the first steps
    - `ARG_TYPE`
        - use `fetch` for downloading individual PubMed IDs
        - use `download` to retrieve FTP update files
    - `ARG_MINYEAR` articles published in years before this will be filtered out (optional)
    - `ARG_TABLE` is the name of the table to dump results into
    - `ARG_THRESHOLD` set the lowest probability to classify an article as 'positive' using [pathway-abstract-classifier](https://github.com/PathwayCommons/pathway-abstract-classifier/)

## Testing

There is a convenience script that can be launched:

```bash
$ ./test.sh
```

This will run the tests in `./tests`, lint with [flake8](https://flake8.pycqa.org/en/latest/) and type check with [mypy](http://mypy-lang.org/).


