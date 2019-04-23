# Configure Tutorial

Create venv:

    python3 -m venv /path/to/new/virtual/environment
    source /path/to/new/virtual/environment/bin/activate

Instal Jupyter with pip:

	pip install --upgrade pip
	pip install --upgrade ipython jupyter

Install dependencies (you may use `setup.py` alternativelly):

    pip install pandas haversine numpy scipy matplotlib sklearn hdbscan xmltodict

Configure API Keys:

Copy the file `.env.example` to `.env` and fill the required keys.

Start the notebook in the tutorial directory:

	cd /path/to/jupyter/folder
	jupyter notebook