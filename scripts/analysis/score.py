import sys
from loguru import logger
from typing import Callable, Generator, List
from pathway_abstract_classifier.pathway_abstract_classifier import Explanation, Classifier
from ncbiutils.pubmed import Citation
from classifier_pipeline.utils import (
    as_pipeline,
    csv2dict_reader,
    list_transformer,
    chunker,
    print_transform,
    exhaust
)
from classifier_pipeline.pubmed import (
    pubmed_transformer
)
import pprint
pp = pprint.PrettyPrinter(indent=4)


def score_transformer(**opts) -> Callable[[Generator[List[Citation], None, None]], Generator[Explanation, None, None]]:
    """Score the output"""
    classifier = Classifier(**opts)

    def _score_transformer(chunks):
        for chunk in chunks:
            explanations = classifier.explain([c.dict() for c in chunk])
            yield from explanations

    return _score_transformer


def sentences_transformer():
    """Rip out the sentences"""
    def _sentences_transformer(explanations):
        for explanation in explanations:
            _, _, sentences  = explanation
            pp.pprint(sentences)
            yield sentences

    return _sentences_transformer


cache = []

def _tee(items):
    for item in items:
        cache.append(item)
        yield item

def _top_sentence(sentences):
    best_text = None
    best_score = None
    for sentence in sentences:
        text = sentence['text']
        score = sentence['score']
        if best_score is None:
            best_text = text
            best_score = score
            continue
        else:
            if score > best_score:
                best_text = text
                best_score = score
    return best_text, best_score



def _fill_input(explanations):
    for explanation in explanations:
        sentences = explanation.sentences
        best_text, best_score = _top_sentence(sentences)
        input = next(c for c in cache if c['pmid'] == explanation.document['pmid'])
        input['title'] = explanation.document['title']
        input['abstract'] = explanation.document['abstract']
        input['best_text'] = best_text
        input['best_score'] = best_score
        yield input

from csv import DictWriter

def _csv_loader(items):
    first = next(items)
    keys = list(first.keys())
    with open('output.csv', 'a') as csvfile:
        writer = DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        writer.writerow(first)
        yield first
        for item in items:
            writer.writerow(item)
            yield item

####################################################
#                 __main__
####################################################

if __name__ == '__main__':

    pipeline = as_pipeline(
        [
            csv2dict_reader(sys.stdin),
            _tee,
            list_transformer(field='pmid'),
            pubmed_transformer(),
            chunker(5),
            score_transformer(),
            sentences_transformer(),
            exhaust,
        ]
    )

    # pipeline = as_pipeline(
    #     [
    #         csv2dict_reader(sys.stdin),
    #         _tee,
    #         list_transformer(field='pmid'),
    #         pubmed_transformer(),
    #         chunker(5),
    #         score_transformer(),
    #         _fill_input,
    #         _csv_loader,
    #         exhaust,
    #     ]
    # )
