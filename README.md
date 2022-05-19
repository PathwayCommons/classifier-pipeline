# classifier-pipeline

A software suite that enables extraction, transformation, filtering and loading of data into a database.

In particular, this repository is geared towards identifying scientific articles containing information about biological pathways.

## Requirements

- [Python (version >=3.8<3.10)](https://www.python.org/)
- [Poetry (version >1.0.0)](https://python-poetry.org/)
- [Docker (version 20.10.14) and Docker Compose (version 2.5.1)](https://www.docker.com/)
  - We use Docker to create a RethinkDB (2.3.6) instance for loading data.
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) (Optional)
  - For creating virtual environments. Any other solution will work.
- Graphics Processing Unit (GPU) (Optional)
  - The [pathway-abstract-classifier](https://github.com/PathwayCommons/pathway-abstract-classifier/) can be sped up an order of magnitude by running on a system with a GPU. We have been using a system running Ubuntu 18.04.5 LTS, Intel(R) Xeon(R) CPU E5-2687W, 24 Core with an NVIDIA GP102 [TITAN Xp] GPU.

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

Configure a pipeline:

- To run a pipeline, edit bash files in the `scripts` directory to extract, transform and load PubMed data from either the [FTP site (`dailyupdates.sh`)](https://www.nlm.nih.gov/databases/download/pubmed_medline.html) or using the [E-Utilities (`pmids.sh`)](https://www.ncbi.nlm.nih.gov/books/NBK25499/).

  - `dailyupdates.sh`: Extract, transform and load daily updates provided  on the PubMed [FTP server](https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/)
    - `DATA_FILE` variable will point to a csv file having a column name (`ARG_IDCOLUMN`). This file column should have  file names you wish to process (e.g. `pubmed22n1115.xml.gz`)
    - `CONDA_ENV` should be the environment name you declared in the first steps
    - `ARG_MINYEAR` articles published in years before this will be filtered out (optional)
    - `ARG_THRESHOLD` set the lowest probability required to classify an article as 'positive' using [pathway-abstract-classifier](https://github.com/PathwayCommons/pathway-abstract-classifier/)
  - `pmids.sh`: Extract, transform and load articles indicated by their PubMed uid
    - `DATA_FILE` variable will point to a csv file having a column name (`ARG_IDCOLUMN`). This file column should have  pmids you wish to process

Simply run the scripts:

```bash
$ ./dailyupdates.sh
```
or

```bash
$ ./pmids.sh
```

## Elements of the 'Pipeline'

The `scripts` directory has a `pipeline.py` script that runs a configurable chain of functions declared in the `classifier_pipeline` module that:
- read in csv data from stdin (`csv2dict_reader`)
- strip out a single column (e.g. PMIDs, filenames) as a list (`list_transformer`)
- retrieves records/files from PubMed (`pubmed_transformer`)
- applies various filters on the individual records (`citation_pubtype_filter`, `citation_date_filter`)
- applies a deep-learning classifier to text fields (`classification_transformer`)
- loads the formatted data into a RethinkDB instance (`db_loader`)


## Testing

There is a convenience script that can be launched:

```bash
$ ./test.sh
```

This will run the tests in `./tests`, lint with [flake8](https://flake8.pycqa.org/en/latest/) and type check with [mypy](http://mypy-lang.org/).


