try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'name': "Minicourse SBRC'2019",
    'version': '0.2',
    'description': 'Exploring hybrid multi-modal urban routes collected from tweets in São Paulo.',
    'author': 'Diego Oliveira and Frances Santos',
    'url': '--',
    'download_url': '--',
    'author_email': '[diego, francessantos]@lrc.ic.unicamp.br',
    'install_requires': ['pandas', 'haversine', 'numpy', 'scipy', 'matplotlib', 'sklearn', 'hdbscan', 'xmltodict', 'beautifulsoup4', 'googlemaps', 'gmplot', 'seaborn']
}

setup(**config)
