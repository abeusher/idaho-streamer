import os.path
from setuptools import setup
from pip.req import parse_requirements
from pip.download import PipSession

install_reqs = parse_requirements(os.path.join(os.path.dirname(__file__), "requirements.txt"),
                                session=PipSession())
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name="idaho-streamer",
    version="0.0.1",
    author="Pramukta Kumar",
    author_email="pramukta.kumar@timbr.io",
    description=("A REST streaming API for DigitalGlobe IDAHO data.  Loosely modeled"
                 "after the Twitter/GNIP Historical Streams, expects the underlying"
                 "MongoDB data source to be populated by something else."),
    license="BSD",
    url="https://github.com/timbr-io/idaho-streamer",
    entry_points = {
        'console_scripts': [
            'idaho_poller = idaho_streamer.gbdx:main',
        ],
    },
    packages=["idaho_streamer"],
    install_requires=reqs
)
