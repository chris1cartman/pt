import uuid
import pandas as pd
from src.io import IOController

class Entity:
    """
    Entities are the most abstract objects with some sort of data base representation
    """

    TYPE = 'abstract'
    REQUIRED_ARGS = []
    ALLOWED_TYPES = [str, int, float]

    def __init__(self, **kwargs):
        """
        Initiates the entity
        :param kwargs: arguments
        """

        # check the attributes
        self._check_args(**kwargs)
        self._check_required_args(**kwargs)

        # add the attributes
        self._attrs = kwargs

        # all entities have an id
        if not 'id' in self._attrs.keys():
            id = str(uuid.uuid4())
            self._attrs.update({'id': id})
            self.store()

    def update_attributes(self, **kwargs):
        """
        Updates arbitrary attributes passed as keyword arguments
        :param kwargs: keyword arguments
        :return: None
        """

        self._check_args(**kwargs)

        # udpate arguments
        self._attrs.update(kwargs)

        # save the attributes in the server
        IOController().update(self)

    @property
    def type(self):
        return self.TYPE

    @property
    def id(self):
        return self._attrs['id']

    def to_df(self):
        dct = {attr: [self._attrs[attr]] for attr in self._attrs.keys()}
        return pd.DataFrame(dct)

    def _check_required_args(self, **kwargs):
        """
        Checks whether all required arguments were received
        :param kwargs: arguments
        :return: None
        """

        for arg in self.REQUIRED_ARGS:
            if arg not in kwargs.keys():
                raise ValueError('Required argument {} not provided'.format(arg))

    def _check_args(self, **kwargs):
        """
        Checks whether the arguments are in the required structure
        :param kwargs: arguments
        :return: None
        """

        for arg in kwargs.keys():
            if type(arg) not in self.ALLOWED_TYPES:
                raise TypeError('Argument {} has invalid type {}'.format(arg, type(arg)))

    def store(self):
        IOController().store(self)

    @classmethod
    def from_store(cls, id):
        dct = IOController().retrieve_by_id(cls.TYPE, id)
        return cls(**dct)


class NamedEntity(Entity):
    REQUIRED_ARGS = ['name']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def name(self):
        return self._attrs['name']


class RelationalEntity(Entity):
    """
    Entities that can be in a one-to-many relationship with other entities
    """

    FILTER = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)



