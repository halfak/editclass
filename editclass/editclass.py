"""
This script provides access to a set of utilities for edit classification

* fetch_quality_scores -- Fetches quality scores for the bounds of article
                          periods
* score_article_periods -- Generates article quality scores using XML dumps
* reverted_status -- Gathers the reverted status of a set of edits.

Usage:
    editclass (-h | --help)
    editclass <utility> [-h | --help]

Options:
    -h | --help  Shows this documentation
    <utility>    The name of the utility to run
"""
import sys
import traceback
from importlib import import_module


USAGE = """Usage:
    editclass (-h | --help)
    editclass <utility> [-h | --help]\n"""


def main():

    if len(sys.argv) < 2:
        sys.stderr.write(USAGE)
        sys.exit(1)
    elif sys.argv[1] in ("-h", "--help"):
        sys.stderr.write(__doc__ + "\n")
        sys.exit(1)
    elif sys.argv[1][:1] == "-":
        sys.stderr.write(USAGE)
        sys.exit(1)

    module_name = sys.argv[1]
    try:
        sys.path.insert(0, ".")
        module = import_module(".utilities." + module_name, package="editclass")
    except ImportError:
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("Could not find utility {0}.\n".format(module_name))
        sys.exit(1)

    module.main(sys.argv[2:])
