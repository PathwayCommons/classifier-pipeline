import pytest
from classifier_pipeline.pubmed import (
    updatefiles_extractor,
    updatefiles_data_filter,
    updatefiles_content2facts_transformer,
    citation_pubtype_filter,
    classification_transformer,
    pubmed_transformer,
    prediction_db_transformer,
    citation_date_filter,
    pmc_supplement_transfomer,
)
from ncbiutils.pubmedxmlparser import Citation
from ncbiutils.ncbiutils import Chunk
from pathway_abstract_classifier.pathway_abstract_classifier import Prediction
import datetime

uids = ('1', '2', '3')
citations = (
    Citation(
        pmid='1',
        title='title1',
        abstract='',
        journal={'pub_year': None},
        publication_type_list=['D016428'],
        correspondence=[],
    ),
    Citation(
        pmid='2',
        title='title2',
        abstract='',
        journal={'pub_year': '1997'},
        publication_type_list=['D016454', 'D016428'],
        correspondence=[],
    ),
    Citation(
        pmid='3',
        title='title3',
        abstract='',
        journal={'pub_year': '2022'},
        publication_type_list=['D055823'],
        correspondence=[],
    ),
)

pmc_chunk_data = Chunk(
    None,
    [
        Citation(
            pmid='33393230',
            pmc='7857436',
            doi='10.15252/embr.202051162',
            title='The deubiquitinase OTUD1 enhances iron transport and potentiates host antitumor immunity',
            abstract='Abstract Although iron is required for cell proliferation, iron‐dependent programmed cell death serves as a critical barrier to tumor growth and metastasis. Emerging evidence suggests that iron‐mediated lipid oxidation also facilitates immune eradication of cancer. However, the regulatory mechanisms of iron metabolism in cancer remain unclear. Here we identify OTUD1 as the deubiquitinase of iron‐responsive element‐binding protein 2 (IREB2), selectively reduced in colorectal cancer. Clinically, downregulation of OTUD1 is highly correlated with poor outcome of cancer. Mechanistically, OTUD1 promotes transferrin receptor protein 1 (TFRC)‐mediated iron transportation through deubiquitinating and stabilizing IREB2, leading to increased ROS generation and ferroptosis. Moreover, the presence of OTUD1 promotes the release of damage‐associated molecular patterns (DAMPs), which in turn recruits the leukocytes and strengthens host immune response. Reciprocally, depletion of OTUD1 limits tumor‐reactive T‐cell accumulation and exacerbates colon cancer progression. Our data demonstrate that OTUD1 plays a stimulatory role in iron transportation and highlight the importance of OTUD1‐IREB2‐TFRC signaling axis in host antitumor immunity. OTUD1 stabilizes IREB2 and enhances TFRC‐mediated iron uptake in intestinal epithelial cells. OTUD1 increases cell susceptibility to ferroptosis and is essential for T cell surveillance against cancer.',
            author_list=[
                {
                    'fore_name': 'Jia',
                    'last_name': 'Song',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0003-1418-3656',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China',
                        '2 Department of Pathology School of Basic Medical Sciences Peking University Health Science Center Beijing China',
                    ],
                    'emails': None,
                },
                {
                    'fore_name': 'Tongtong',
                    'last_name': 'Liu',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0003-4979-8220',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China'
                    ],
                    'emails': None,
                },
                {
                    'fore_name': 'Yue',
                    'last_name': 'Yin',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0002-7998-8597',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China'
                    ],
                    'emails': None,
                },
                {
                    'fore_name': 'Wei',
                    'last_name': 'Zhao',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0003-2498-1272',
                    'affiliations': [
                        '3 Department of Clinical Laboratory China‐Japan Friendship Hospital Beijing China'
                    ],
                    'emails': None,
                },
                {
                    'fore_name': 'Zhiqiang',
                    'last_name': 'Lin',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0003-1834-2060',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China'
                    ],
                    'emails': None,
                },
                {
                    'fore_name': 'Yuxin',
                    'last_name': 'Yin',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0003-4102-0043',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China'
                    ],
                    'emails': ['yinyuxin@bjmu.edu.cn'],
                },
                {
                    'fore_name': 'Dan',
                    'last_name': 'Lu',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0002-3000-5094',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China'
                    ],
                    'emails': ['taotao@bjmu.edu.cn'],
                },
                {
                    'fore_name': 'Fuping',
                    'last_name': 'You',
                    'initials': None,
                    'collective_name': None,
                    'orcid': 'https://orcid.org/0000-0002-7444-729X',
                    'affiliations': [
                        '1 Institute of Systems Biomedicine School of Basic Medical Sciences Peking University Health Science Center Beijing China'
                    ],
                    'emails': ['fupingyou@bjmu.edu.cn'],
                },
            ],
            journal={
                'title': 'EMBO Reports',
                'issn': ['1469-221X', '1469-3178'],
                'volume': '22',
                'issue': '2',
                'pub_year': '2021',
                'pub_month': '1',
                'pub_day': '04',
            },
            publication_type_list=[],
            correspondence=[
                {
                    'emails': ['yinyuxin@bjmu.edu.cn', 'taotao@bjmu.edu.cn', 'fupingyou@bjmu.edu.cn'],
                    'notes': '* Corresponding author. Tel: +86 010 82805570; E‐mail: yinyuxin@bjmu.edu.cn Corresponding author. Tel: +86 010 82805807; E‐mail: taotao@bjmu.edu.cn Corresponding author. Tel: +86 010 82805340; E‐mail: fupingyou@bjmu.edu.cn',
                }
            ],
        )
    ],
    ['7857436'],
)

pmc_docs_data = [
    {
        'pmid': '33157209',
        'pmc': None,
        'doi': '10.1016/j.freeradbiomed.2020.10.307',
        'title': 'Ubiquitin-specific protease 7 promotes ferroptosis via activation of the p53/TfR1 pathway in the rat hearts after ischemia/reperfusion.',
        'abstract': 'Iron overload triggers the ferroptosis in the heart following ischemia/reperfusion (I/R) and transferrin receptor 1 (TfR1) charges the cellular iron uptake. Bioinformatics analysis shows that the three molecules of ubiquitin-specific protease 7 (USP7), p53 and TfR1 form a unique pathway of USP7/p53/TfR1. This study aims to explore whether USP7/p53/TfR1 pathway promotes ferroptosis in rat hearts suffered I/R and the underlying mechanisms. The SD rat hearts were subjected to 1 h-ischemia plus 3 h-reperfusion, showing myocardial injury (increase in creatine kinase release, infarct size, myocardial fiber loss and disarray) and up-regulation of USP7, p53 and TfR1 concomitant with an increase of ferroptosis (reflecting by accumulation of iron and lipid peroxidation while decrease of glutathione peroxidase activity). Inhibition of USP7 activated p53 via suppressing deubiquitination, which led to down-regulation of TfR1, accompanied by the decreased ferroptosis and myocardial I/R injury. Next, H9c2 cells underwent hypoxia/reoxygenation (H/R) in vitro to mimic the myocardial I/R model in vivo. Consistent with the results in vivo, inhibition or knockdown of USP7 reduced the H/R injury (decrease of LDH release and necrosis) and enhanced the ubiquitination of p53 along with the decreased levels of p53 and TfR1 as well as the attenuated ferroptosis (manifesting as the decreased iron content and lipid peroxidation while the increased GPX activity). Knockdown of TfR1 inhibited H/R-induced ferroptosis without p53 deubiquitination. Based on these observations, we conclude that a novel pathway of USP7/p53/TfR1 has been identified in the I/R-treated rat hearts, where up-regulation of USP7promotes ferrptosis via activation of the p53/TfR1 pathway.',
        'author_list': [
            {
                'fore_name': 'Li-Jing',
                'last_name': 'Tang',
                'initials': 'LJ',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Pharmacology, Xiangya School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China; Department of Laboratory Medicine, The Third Xiangya Hospital, Central South University, Changsha, 410013, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Yuan-Jing',
                'last_name': 'Zhou',
                'initials': 'YJ',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Pharmacology, Xiangya School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Xiao-Ming',
                'last_name': 'Xiong',
                'initials': 'XM',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Pharmacology, Xiangya School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China; Hunan Provincial Key Laboratory of Cardiovascular Research, School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Nian-Sheng',
                'last_name': 'Li',
                'initials': 'NS',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Pharmacology, Xiangya School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Jie-Jie',
                'last_name': 'Zhang',
                'initials': 'JJ',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Obstetrics, Xiangya Hospital Central South University, Changsha, China; Hunan Engineering Research Center of Early Life Development and Disease Prevention, Changsha, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Xiu-Ju',
                'last_name': 'Luo',
                'initials': 'XJ',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Laboratory Medicine, The Third Xiangya Hospital, Central South University, Changsha, 410013, China. Electronic address: xjluo22@csu.edu.cn.'
                ],
                'emails': ['xjluo22@csu.edu.cn'],
            },
            {
                'fore_name': 'Jun',
                'last_name': 'Peng',
                'initials': 'J',
                'collective_name': None,
                'orcid': None,
                'affiliations': [
                    'Department of Pharmacology, Xiangya School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China; Hunan Provincial Key Laboratory of Cardiovascular Research, School of Pharmaceutical Sciences, Central South University, Changsha, 410078, China. Electronic address: Junpeng@csu.edu.cn.'
                ],
                'emails': ['Junpeng@csu.edu.cn'],
            },
        ],
        'journal': {
            'title': 'Free radical biology & medicine',
            'issn': ['1873-4596'],
            'volume': '162',
            'issue': None,
            'pub_year': '2021',
            'pub_month': '01',
            'pub_day': None,
        },
        'publication_type_list': ['D016428', 'D013485'],
        'correspondence': [],
        'id': '33157209',
        'classification': 1,
        'probability': 0.9553418159484863,
    },
    {
        'pmid': '33393230',
        'pmc': 'PMC7857436',
        'doi': '10.15252/embr.202051162',
        'title': 'The deubiquitinase OTUD1 enhances iron transport and potentiates host antitumor immunity.',
        'abstract': 'Although iron is required for cell proliferation, iron-dependent programmed cell death serves as a critical barrier to tumor growth and metastasis. Emerging evidence suggests that iron-mediated lipid oxidation also facilitates immune eradication of cancer. However, the regulatory mechanisms of iron metabolism in cancer remain unclear. Here we identify OTUD1 as the deubiquitinase of iron-responsive element-binding protein 2 (IREB2), selectively reduced in colorectal cancer. Clinically, downregulation of OTUD1 is highly correlated with poor outcome of cancer. Mechanistically, OTUD1 promotes transferrin receptor protein 1 (TFRC)-mediated iron transportation through deubiquitinating and stabilizing IREB2, leading to increased ROS generation and ferroptosis. Moreover, the presence of OTUD1 promotes the release of damage-associated molecular patterns (DAMPs), which in turn recruits the leukocytes and strengthens host immune response. Reciprocally, depletion of OTUD1 limits tumor-reactive T-cell accumulation and exacerbates colon cancer progression. Our data demonstrate that OTUD1 plays a stimulatory role in iron transportation and highlight the importance of OTUD1-IREB2-TFRC signaling axis in host antitumor immunity.',
        'author_list': [
            {
                'fore_name': 'Jia',
                'last_name': 'Song',
                'initials': 'J',
                'collective_name': None,
                'orcid': '0000-0003-1418-3656',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.',
                    'Department of Pathology, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.',
                ],
                'emails': None,
            },
            {
                'fore_name': 'Tongtong',
                'last_name': 'Liu',
                'initials': 'T',
                'collective_name': None,
                'orcid': '0000-0003-4979-8220',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Yue',
                'last_name': 'Yin',
                'initials': 'Y',
                'collective_name': None,
                'orcid': '0000-0002-7998-8597',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Wei',
                'last_name': 'Zhao',
                'initials': 'W',
                'collective_name': None,
                'orcid': '0000-0003-2498-1272',
                'affiliations': [
                    'Department of Clinical Laboratory, China-Japan Friendship Hospital, Beijing, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Zhiqiang',
                'last_name': 'Lin',
                'initials': 'Z',
                'collective_name': None,
                'orcid': '0000-0003-1834-2060',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Yuxin',
                'last_name': 'Yin',
                'initials': 'Y',
                'collective_name': None,
                'orcid': '0000-0003-4102-0043',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Dan',
                'last_name': 'Lu',
                'initials': 'D',
                'collective_name': None,
                'orcid': '0000-0002-3000-5094',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.'
                ],
                'emails': None,
            },
            {
                'fore_name': 'Fuping',
                'last_name': 'You',
                'initials': 'F',
                'collective_name': None,
                'orcid': '0000-0002-7444-729X',
                'affiliations': [
                    'Institute of Systems Biomedicine, School of Basic Medical Sciences, Peking University Health Science Center, Beijing, China.'
                ],
                'emails': None,
            },
        ],
        'journal': {
            'title': 'EMBO reports',
            'issn': ['1469-3178'],
            'volume': '22',
            'issue': '2',
            'pub_year': '2021',
            'pub_month': '02',
            'pub_day': '03',
        },
        'publication_type_list': ['D016428', 'D013485'],
        'correspondence': [],
        'id': '33393230',
        'classification': 1,
        'probability': 0.9969717264175415,
    },
]


@pytest.fixture
def citation_items():
    return (c for c in citations)


@pytest.fixture
def citation_chunks():
    yield [citations]


@pytest.fixture
def prediction_items():
    c_as_dicts = [c.dict() for c in citations]
    p0 = Prediction(document=c_as_dicts[0], classification=1, probability=1)
    p1 = Prediction(document=c_as_dicts[1], classification=1, probability=1)
    return (p for p in [p0, p1])


@pytest.fixture
def uid_items():
    return (c for c in uids)


@pytest.fixture
def citations_chunks():
    error = None
    yield [(error, citations, uids)]


@pytest.fixture
def updatefiles_contents():
    return (c for c in citations)


@pytest.fixture
def pmc_citation_chunks():
    yield [pmc_chunk_data]


@pytest.fixture
def pmc_docs():
    yield [pmc_docs_data]


####################################################
#                  Extractor
####################################################


def test_updatefiles_extractor(mocker, list_contents):
    mocker.patch('classifier_pipeline.pubmed.ftp.Ftp.list', return_value=list_contents)
    contents = [c for c in updatefiles_extractor()]
    assert len(contents) == len(list_contents)


####################################################
#                  Filter
####################################################


def test_updatefiles_data_filter(list_contents):
    filtered = updatefiles_data_filter()(list_contents)
    for content in filtered:
        name, _ = content
        assert name != '.'
        assert name != '..'
        assert name != 'README.txt'
        assert '.md5' not in name
        assert '.html' not in name


def test_citation_pubtype_filter(citation_items):
    results = list(citation_pubtype_filter(citation_items))
    assert len(results) == 1


def test_citation_date_filter(citation_items):
    citations = list(citation_date_filter(2021)(citation_items))
    assert len(citations) == 1


def test_citation_date_filter_disabled(citation_items):
    citations = list(citation_date_filter()(citation_items))
    assert len(citations) == 3


####################################################
#                  Transform
####################################################


def test_updatefiles_content2facts_transformer(list_contents):
    transformed = updatefiles_content2facts_transformer(list_contents)
    for item in transformed:
        assert 'id' in item
        assert 'filename' in item


def test_classification_transformer(mocker, citation_chunks, prediction_items):
    mocker.patch('classifier_pipeline.pubmed.Classifier.predict', return_value=prediction_items)
    predictions = classification_transformer()(citation_chunks)
    p_list = list(predictions)
    assert p_list is not None
    assert len(p_list) == 2


def test_pubmed_transformer(mocker, uid_items, citations_chunks):
    mocker.patch('classifier_pipeline.pubmed.PubMedFetch.get_citations', return_value=citations_chunks)
    citations = list(pubmed_transformer()(uid_items))
    assert len(citations) == 3


def test_prediction_db_transformer(prediction_items):
    formatted = list(prediction_db_transformer()(prediction_items))
    for item in formatted:
        assert 'id' in item
        assert 'pub_date' in item
        if item['pub_date'] is not None:
            assert isinstance(item['pub_date'], datetime.date)


def test_pmc_supplement_transfomer(mocker, pmc_docs, pmc_citation_chunks):
    mocker.patch('classifier_pipeline.pubmed.PubMedFetch.get_citations', return_value=pmc_citation_chunks)
    merged = list(pmc_supplement_transfomer()(pmc_docs))
    result = next(r for r in merged if r['pmid'] == '33393230')
    assert result['pmc'] == 'PMC7857436'
    assert len(result['correspondence']) > 0

    author_list = result['author_list']
    author1 = next(a for a in author_list if a['fore_name'] == 'Yuxin' and a['last_name'] == 'Yin')
    assert len(author1['emails']) > 0
    assert 'yinyuxin@bjmu.edu.cn' in author1['emails']
    author2 = next(a for a in author_list if a['fore_name'] == 'Dan' and a['last_name'] == 'Lu')
    assert len(author2['emails']) > 0
    assert 'taotao@bjmu.edu.cn' in author2['emails']
