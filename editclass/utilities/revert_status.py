"""
Gathers reverting and reverted status.  If the edit was reverted, it's scored
as vandalism or not.

* rev_id
* reverting
* reverted
* reverted_score

Usage:
    reverted_status -h | --help
    reverted_status [--radius=<revs>] [--window=<hours>]
                    [--revisions=<path>] [--output=<path>]

Options:
    -h --help           Prints this documentation
    --radius=<revs>     The maximum number of revisions that can be reverted
                        in a revert [default: 15]
    --window=<hours>    The maximum hours after a revision is saved where it
                        can be reverted. [default: 48]
    --revisions=<path>  The path to a file containing rev_ids to lookup.
                        [default: <stdin>]
    --output=<path>     The path to a file to write output to.
                        [default: <stdout>]
"""
import os.path
import sys
from itertools import chain

import docopt
import mwtypes
import requests
from mw import database

import mwreverts
import mysqltsv

HEADERS = ['rev_id', 'reverting', 'reverted', 'score']


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)

    if args['--revisions'] == "<stdin>":
        revisions = mysqltsv.Reader(sys.stdin, headers=['rev_id'])
    else:
        path = os.path.expanduser(args['--revisions'])
        revisions = mysqltsv.Reader(open(path), headers=['rev_id'])

    if args['--output'] == "<stdout>":
        output = mysqltsv.Writer(sys.stdout, headers=HEADERS)
    else:
        path = os.path.expanduser(args['--output'])
        output = mysqltsv.Writer(open(path, "w"), headers=HEADERS)

    radius = int(args['--radius'])

    window = int(args['--window'])*60*60  # converts hours to seconds

    run(revisions, output, radius, window)

ORES_ENWIKI = "http://ores.wmflabs.org/scores/enwiki/"


def run(revisions, output, radius, window):

    # Connect to DB
    db = database.DB.from_params(
        host="analytics-store.eqiad.wmnet",
        user="research",
        database="enwiki",
        defaults_file="~/.my.cnf"
    )

    for revision in revisions:
        reverting, reverted = \
            get_revert_status(db, revision.rev_id, radius, window)

        if reverted:
            sys.stderr.write("r")
            sys.stderr.flush()
            response = requests.get(ORES_ENWIKI + "reverted/{0}/"
                                    .format(revision.rev_id))

            doc = response.json()

            score = doc[str(revision.rev_id)]['probability']['true']

        else:
            sys.stderr.write(".")
            sys.stderr.flush()
            score = None

        output.write([revision.rev_id, reverting, reverted, score])


def get_revert_status(db, rev_id, radius, window):

    row = db.revisions.get(rev_id=rev_id)
    page_id = row['rev_page']

    # Load history and current rev
    current_and_past_revs = list(db.revisions.query(
        page_id=page_id,
        limit=radius + 1,
        before_id=rev_id + 1,  # Ensures that we capture the current revision
        direction="older"
    ))

    try:
        # Extract current rev and reorder history
        current_rev, past_revs = (
            current_and_past_revs[0],  # Current rev is the first one returned
            reversed(current_and_past_revs[1:])  # The rest are past revs
        )
    except IndexError:
        # Only way to get here is if there isn't enough history.  Couldn't be
        # reverted.  Just return None.
        return None

    before = mwtypes.Timestamp(current_rev['rev_timestamp']) + window

    # Load future revisions
    future_revs = db.revisions.query(
        page_id=page_id,
        limit=radius,
        after_id=rev_id,
        before=before,
        direction="newer"
    )

    # Convert to an iterable of (checksum, rev) pairs for detect() to consume
    checksum_revisions = chain(
        ((rev['rev_sha1'] if rev['rev_sha1'] is not None
          else mwreverts.DummyChecksum(), rev)
         for rev in past_revs),
        [(current_rev['rev_sha1'] or DummyChecksum(), current_rev)],
        ((rev['rev_sha1'] if rev['rev_sha1'] is not None
          else mwreverts.DummyChecksum(), rev)
         for rev in future_revs)
    )

    reverted = False
    reverting = False
    for revert in mwreverts.detect(checksum_revisions, radius=radius):
        # Check that this is a relevant revert
        if rev_id in {rev['rev_id'] for rev in revert.reverteds}:
            reverted = True

        if rev_id == revert.reverting['rev_id']:
            reverting = True

    return reverting, reverted
