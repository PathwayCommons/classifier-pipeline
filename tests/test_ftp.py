import pytest

from classifier_pipeline.ftp import Ftp

NCBI_FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
NCBI_PUBMED_FTP_PATH = 'pubmed/updatefiles'
EMAIL = 'info@biofactoid.org'


@pytest.fixture
def list_items():
    items = [
        (
            'pubmed22n1323.xml.gz',
            {
                'modify': '20220518180915',
                'perm': 'adfr',
                'size': '60',
                'type': 'file',
                'unique': '3DU2A30F5C1',
                'unix.group': '528',
                'unix.groupname': 'anonymous',
                'unix.mode': '0444',
                'unix.owner': '14',
                'unix.ownername': 'ftp',
            },
        ),
        (
            'pubmed22n1323.xml.gz.md5',
            {
                'modify': '20220518180915',
                'perm': 'adfr',
                'size': '60',
                'type': 'file',
                'unique': '3DU2A30F5C1',
                'unix.group': '528',
                'unix.groupname': 'anonymous',
                'unix.mode': '0444',
                'unix.owner': '14',
                'unix.ownername': 'ftp',
            },
        ),
        (
            'pubmed22n1323_stats.html',
            {
                'modify': '20220518180915',
                'perm': 'adfr',
                'size': '586',
                'type': 'file',
                'unique': '3DU2A30F5C2',
                'unix.group': '528',
                'unix.groupname': 'anonymous',
                'unix.mode': '0444',
                'unix.owner': '14',
                'unix.ownername': 'ftp',
            },
        ),
    ]
    return items


class TestFtpInstance:
    ftp = Ftp(host=NCBI_FTP_HOST, passwd=EMAIL)

    def test_set_db_attr(self):
        assert self.ftp.host == NCBI_FTP_HOST
        assert self.ftp.passwd == EMAIL
        assert self.ftp.port == 21
        assert self.ftp.user == 'anonymous'

    def test_ftp_list(self, list_items, mocker):
        mocker.patch('classifier_pipeline.ftp.Ftp.list', return_value=list_items)
        contents = self.ftp.list(NCBI_PUBMED_FTP_PATH)
        assert len(contents) > 0
        name, facts = contents[0]
        assert isinstance(name, str)
        assert isinstance(facts, dict)
