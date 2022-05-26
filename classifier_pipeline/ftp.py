from pydantic import BaseModel, PrivateAttr
from typing import Any, Tuple, Dict, List
from ftplib import FTP
from loguru import logger


class Ftp(BaseModel):
    """
    Helper class for read-access to resources

    Class attributes
    ----------

    Attributes
    ----------
    host : str = 'localhost'
        Database host
    passwd : Optional[str]
        Ftp password
    port : int = 21
        Port
    user : str = 'anonymous'
        Ftp user name


    Methods
    ----------
    list(path:str) -> List[Tuple]:
        List files in the remote directory under the provided path
    """

    host: str
    passwd: str
    port: int = 21
    user: str = 'anonymous'

    _client: Any = PrivateAttr()

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._client = FTP()

    def _connect(self) -> bool:
        try:
            self._client.connect(host=self.host, port=self.port)
            self._client.login(user=self.user, passwd=self.passwd)
            return True
        except Exception as e:
            logger.error('Error connecting: {e}', e=e)
            return False

    def list(self, path: str) -> List[Tuple[str, Dict[str, str]]]:
        """List the contents of the directory under path and yield tuple (filename, facts):

         (<file name>,
            {'modify': 'yyyymmddhhmmss',
            'perm': 'adfr',
            'size': '22287188',
            'type': 'file',
            'unique': '3BU5B4FF5C3',
            'unix.group': '528',
            'unix.groupname': 'anonymous',
            'unix.mode': '0444',
            'unix.owner': '14',
            'unix.ownername': 'ftp'}
        )

        see https://tools.ietf.org/html/rfc3659.html
        """
        contents = None
        try:
            connected = self._connect()
            if connected:
                response = self._client.mlsd(path)
                contents = [f for f in response]
            else:
                raise
        except Exception as e:
            logger.error('Error in list {e}', e=e)
            raise e
        else:
            self._client.close()
            return contents
