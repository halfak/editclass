"""
Generates article quality scores a specific period within an article's history
using XML dumps to gather article text.

Usage:
    score_article_periods <model-file> <dump-file>...

Options:
    -h --help     Prints this documentation
    <model-file>  The path to a `revscoring` scorer model file
    <dump-file>   The path to a MediaWiki XML dump file to process
"""
import logging
import sys

import docopt
import mwxml
from revscoring.datasources import revision
from revscoring.scorer_models import MLScorerModel

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
    page_periods = {p.page_id: (p.start_rev_id, p.end_rev_id) for p in periods}

    scorer_model = MLScorerModel.load(open(args['<model-file>'], 'rb'))

    dump_paths = args['<dump-file>']

    run(page_periods, scorer_model, dump_paths)

def run(page_periods, scorer_model, dump_paths):

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
    )

    writer = mysqltsv.Writer(sys.stdout, headers=HEADERS)

    def process_dump(dump, path):

        for page in dump:
            if page.namespace != 0 or page.id not in page_periods:
                continue
            else:
                start_id, end_id = page_periods[page.id]

            #sys.stderr.write(page.title + ": ");sys.stderr.flush()

            pre_period_revision = None
            for revision in page:
                if revision.id < start_id:
                    #sys.stderr.write(".");sys.stderr.flush()
                    pre_period_revision = revision

                if revision.id == end_id:
                    if pre_period_revision is not None:
                        start_text = pre_period_revision.text
                        start_id = pre_period_revision.id
                    else:
                        start_text = ""
                        start_id = None
                    
                    start_score = generate_score(scorer_model, start_text)
                    #sys.stderr.write("s1");sys.stderr.flush()
                    end_score = generate_score(scorer_model, revision.text)
                    #sys.stderr.write("s2");sys.stderr.flush()
                    yield (page.id,
                           pre_period_revision.id,
                           start_score['prediction'], weighted_sum(start_score),
                           revision.id,
                           end_score['prediction'], weighted_sum(end_score))

                    break

            #sys.stderr.write("\n")

    for values in mwxml.map(process_dump, dump_paths):
        writer.write(values)


def generate_score(scorer_model, text):
    feature_values = scorer_model.language.solve(scorer_model.features,
                                                 cache={revision.text: text})
    return scorer_model.score(list(feature_values))

def weighted_sum(score):
    return sum(p*CLASS_VALUES[k] for k, p in score['probability'].items())
