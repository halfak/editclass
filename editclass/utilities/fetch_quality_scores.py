"""
Gets quality scores for the described periods of an article.

Usage:
    fetch_quality_scores -h | --help
    fetch_quality_scores [--ores=<url>] [--mwapi=<url>]

Options:
    -h --help      Prints this documentation
    --ores=<url>   The base URL of an ORES installation to use when looking up
                   scores [default: https://ores.wmflabs.org]
    --mwapi=<url>  The base URL for a MediaWiki API.
                   [default: https://en.wikipedia.org/w/api.php]
"""
import sys
import time
import traceback
from statistics import mean

import docopt
import requests
from mw import api

import mysqltsv

HEADERS = [
    "page_id",
    "prev_rev_id",
    "prev_prediction",
    "prev_weighted_sum",
    "end_rev_id",
    "end_prediction",
    "prev_weighted_sum"
]

CLASS_VALUES = {
    "FA": 5,
    "GA": 4,
    "B": 3,
    "C": 2,
    "Start": 1,
    "Stub": 0
}


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    periods = mysqltsv.read(sys.stdin, types=[int, str, int, int, int])

    my_agent = "Quality Scores Script <ahalfaker@wikimedia.org>"
    session = api.Session(args['--mwapi'], user_agent=my_agent)

    ores = ORESScorer(args['--ores'])

    run(periods, ores, session)

def run(periods, ores, session):

    writer = mysqltsv.Writer(sys.stdout, headers=HEADERS)

    for period in periods:
        # Get previous revision
        revs = session.revisions.query(pageids=[period.page_id],
                                       start_id=period.start_rev_id-1,
                                       direction="older", limit=1)

        revs = list(revs)
        if len(revs) == 0:
            sys.stderr.write("?")
            continue

        prev_rev_id = revs[0]['revid']
        try:
            prev_score = ores.score(prev_rev_id)
            end_score = ores.score(period.end_rev_id)

            writer.write([
                period.page_id,
                prev_rev_id,
                prev_score['prediction'],
                weighted_sum(prev_score),
                period.end_rev_id,
                end_score['prediction'],
                weighted_sum(end_score)
            ])
            sys.stderr.write(".");
            sys.stderr.flush()
        except Exception as e:
            sys.stderr.write(traceback.format_exc())
            sys.stderr.write("\n")



    sys.stderr.write("\n")

class ORESScorer:

    def __init__(self, url):
        self.url = url

    def score(self, rev_id):
        response = requests.get(self.url + "/scores/enwiki/wp10/" + str(rev_id))
        return response.json()[str(rev_id)]


def weighted_sum(score):
    return sum(p*CLASS_VALUES[k] for k, p in score['probability'].items())
