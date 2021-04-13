# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License",
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for working with read concerns."""

from bson.timestamp import Timestamp

class ReadConcern(object):
    """ReadConcern

    :Parameters:
        - `level`: (string) The read concern level specifies the level of
          isolation for read operations.  For example, a read operation using a
          read concern level of ``majority`` will only return data that has been
          written to a majority of nodes. If the level is left unspecified, the
          server default will be used.
        - `atClusterTime`: (timestamp) The timestamp at which a read with
          readConcern `snapshot` reads from.

    .. versionadded:: 3.2

    """

    def __init__(self, level=None, atClusterTime=None):
        if level is None or isinstance(level, str):
            self.__level = level
        else:
            raise TypeError(
                'level must be a string or None.')

        if atClusterTime is None or isinstance(atClusterTime, Timestamp):
            self.__atClusterTime = atClusterTime
        else:
            raise TypeError(
                'atClusterTime must be a Timestamp or None.')
    
    @property
    def level(self):
        """The read concern level."""
        return self.__level

    @property
    def atClusterTime(self):
        """The atClusterTime."""
        return self.__atClusterTime

    @property
    def ok_for_legacy(self):
        """Return ``True`` if this read concern is compatible with
        old wire protocol versions."""
        return self.level is None or self.level == 'local'

    @property
    def document(self):
        """The document representation of this read concern.

        .. note::
          :class:`ReadConcern` is immutable. Mutating the value of
          :attr:`document` does not mutate this :class:`ReadConcern`.
        """
        doc = {}
        if self.__level:
            doc['level'] = self.level
        if self.__atClusterTime:
            doc['atClusterTime'] = self.__atClusterTime
        return doc

    def __eq__(self, other):
        if isinstance(other, ReadConcern):
            return self.document == other.document
        return NotImplemented

    def __repr__(self):
        if self.level:
            return 'ReadConcern(%s)' % self.level
        return 'ReadConcern()'


DEFAULT_READ_CONCERN = ReadConcern()
