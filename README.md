# Hybrid Urban Routing Tutorial SBRC

This tutorial was produced as a suplementary material for the short course "Computação Urbana da Teoria à Prática: Fundamentos, Aplicações e Desafios" presented at the [SBRC'19](http://sbrc2019.sbc.org.br/en/). The aim is to familiarize the audience with the Urban Computing framework by usan urban mobility data to identify and analyse mobility flows.

## Requirements

You may use a GIT client to download this tutorial from Github. Windows OS usually do not come with GIT out of the box, so you may need to install it. Also, if you do not have Python 3 on your machine you have to install.

Some Python 3 installations may not come with support for virtual environments. Thus, you may need to install some libs:

    sudo apt-get install python3.6-dev python3-venv

## Configure Tutorial

Download the tutorial from Github using GIT.

    git clone https://github.com/diegopso/hybrid-urban-routing-tutorial-sbrc.git

Create venv:

    python3 -m venv workspace
    source workspace/bin/activate

Instal Jupyter with pip:

	pip install --upgrade pip
	pip install --upgrade ipython jupyter

Install dependencies (you may use `setup.py` alternativelly):

	cd hybrid-urban-routing-tutorial-sbrc
    pip install .

Configure API Keys:

Copy the file `.env.example` to `.env` and fill the required keys.

    cp .env.example .env
    nano .env

A sample file containig API Keys for use will be available for the tutorial at the hosting event. However the keys will be invalidated after its occurrence. To access this file use the following URL: [https://www.dropbox.com/s/mbbvmai277giegd/env.txt?dl=0](https://www.dropbox.com/s/mbbvmai277giegd/env.txt?dl=0).

To create API Keys you have to visit every provider's website:

* [Uber API](https://developer.uber.com/docs/riders/ride-requests/tutorials/api/introduction)
* [Google Directions API](https://developers.google.com/maps/documentation/directions/get-api-key)
* [TomTom Developer API](https://developer.tomtom.com/user/register)

Start the notebook in the tutorial directory:

	jupyter notebook
	

