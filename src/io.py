import pandas as pd
import os


class Singleton(type):
    instance = None

    def __call__(cls, *args, **kw):
        if not cls.instance:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance

    def reset(cls):
        cls.instance = None


class IOController(metaclass=Singleton):
    """
    Controls all IOs
    """

    ABSTRACT_DF = os.path.join('data', 'server', 'abstract.csv')
    PERSON_DF = os.path.join('data', 'server', 'persons.csv')
    PERSONS_FOLDER = os.path.join('data', 'server', 'persons')
    GROUPS_DF = os.path.join('data', 'server', 'groups.csv')
    GROUPS_FOLDER = os.path.join('data', 'server', 'groups')
    PAYMENTS_DF = os.path.join('data', 'server', 'payments.csv')
    PAYMENTS_FOLDER = os.path.join('data', 'server', 'payments')

    def __init__(self):
        """
        Initiates a singleton IO controller
        """
        self._request_queue = []

    def _retrieve_file_name(self, entity_type):
        """
        Retrieves the filename for the entity list of a certain entity type
        :param entity_type: string - indicates the entity type
        :return: list
        """

        # get the correct filename
        if entity_type == 'abstract':
            fn = self.ABSTRACT_DF
        elif entity_type == 'person':
            fn = self.PERSON_DF
        elif entity_type == 'group':
            fn = self.GROUPS_DF
        elif entity_type == 'payment':
            fn = self.PAYMENTS_DF
        else:
            raise TypeError('Entity type {} does not exist'.format(entity_type))

        return fn

    def _save(self, df, filename):
        """
        Saves a file
        :param filename: string - filename
        :return: None
        """

        dir = os.path.dirname(filename)
        if not os.path.exists(dir):
            os.makedirs(dir)
        df.to_csv(filename, index=False)

    def retrieve_entity_list(self, entity_type):
        """
        Retrieves the list of stored entities of a certain type
        :param entity_type: string - indicates the entity type
        :return: list
        """

        # read the file and return
        try:
            df = pd.read_csv(self._retrieve_file_name(entity_type))
        except FileNotFoundError:
            df = pd.DataFrame(columns=['id'])
        df.index = df.loc[:, 'id']
        return df

    def store(self, entity):
        """
        Stores an entity in a list
        :param entity: entity to be stored
        :return: None
        """

        current_list = self.retrieve_entity_list(entity.type)
        current_list = current_list.append(entity.to_df())
        self._save(current_list, self._retrieve_file_name(entity.type))

    def update(self, entity):
        """
        Updates a stored entity
        :param entity: entity to be updated
        :return: None
        """

        current_list = self.retrieve_entity_list(entity.type)
        current_list.drop(entity.id, axis=0, inplace=True)
        current_list = current_list.append(entity.to_df())
        self._save(current_list, self._retrieve_file_name(entity.type))

    def retrieve_by_id(self, entity_type, id):
        """
        Retrieves entity raw data as a dictionary by its id
        :param entity_type: string - indicates the entity type
        :param id: string - id
        :return: raw data dictionary
        """

        current_list = self.retrieve_entity_list(entity_type)
        return current_list.loc[current_list.index == id].iloc[0].to_dict()








