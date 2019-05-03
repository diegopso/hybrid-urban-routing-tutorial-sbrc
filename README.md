# Hybrid Urban Routing Tutorial SBRC

This tutorial was produced as a suplementary material for the short course "Computação Urbana da Teoria à Prática: Fundamentos, Aplicações e Desafios" presented at the [SBRC'19](http://sbrc2019.sbc.org.br/en/). The aim is to familiarize the audience with the Urban Computing framework by usan urban mobility data to identify and analyse mobility flows.

## Requirements

You may use a GIT client to download this tutorial from Github. Windows OS usually do not come with GIT out of the box, so you may need to install it. Also, if you do not have Python 3 on your machine you have to install.

Also, some Python 3 instalations do not come with support for virtual envs, thus you may need to install the following lib:

    apt get install python3-dev

## Configure Tutorial

Download the tutorial from Github using GIT.

    git clone https://github.com/diegopso/hybrid-urban-routing-tutorial-sbrc.git

Create venv:

    python3 -m venv /path/to/new/virtual/environment
    source /path/to/new/virtual/environment/bin/activate

Instal Jupyter with pip:

	pip install --upgrade pip
	pip install --upgrade ipython jupyter

Install dependencies (you may use `setup.py` alternativelly):

    pip install .

Configure API Keys:

Copy the file `.env.example` to `.env` and fill the required keys.

    cp .env.example .env
    nano .env

Start the notebook in the tutorial directory:

	cd /path/to/jupyter/folder
	jupyter notebook
