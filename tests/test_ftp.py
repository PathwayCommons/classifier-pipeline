from classifier_pipeline.ftp import Ftp

NCBI_FTP_HOST = 'ftp.ncbi.nlm.nih.gov'
NCBI_PUBMED_FTP_PATH = 'pubmed/updatefiles'
EMAIL = 'info@biofactoid.org'


class TestFtpInstance:
    ftp = Ftp(host=NCBI_FTP_HOST, passwd=EMAIL)

    def test_set_db_attr(self):
        assert self.ftp.host == NCBI_FTP_HOST
        assert self.ftp.passwd == EMAIL
        assert self.ftp.port == 21
        assert self.ftp.user == 'anonymous'

    def test_ftp_list(self, list_contents, mocker):
        mocker.patch('classifier_pipeline.ftp.Ftp.list', return_value=list_contents)
        contents = self.ftp.list(NCBI_PUBMED_FTP_PATH)
        assert len(contents) > 0
        name, facts = contents[0]
        assert isinstance(name, str)
        assert isinstance(facts, dict)
