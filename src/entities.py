import uuid
import pandas as pd
import numpy as np
from src.io import IOController

class Entity:
    """
    Entities are the most abstract objects with some sort of data base representation
    """

    TYPE = 'abstract'
    REQUIRED_ARGS = []
    ALLOWED_TYPES = [str, int, float, np.float64]

    def __init__(self, store=True, **kwargs):
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
            if store:
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
        self.update_in_store()

    @property
    def type(self):
        return self.TYPE

    @property
    def id(self):
        return self._attrs['id']

    @property
    def attrs(self):
        return self._attrs.copy()

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
            if type(kwargs[arg]) not in self.ALLOWED_TYPES:
                raise TypeError('Argument {} has invalid type {}'.format(arg, type(kwargs[arg])))

    def store(self):
        IOController().store(self)

    def delete_from_store(self):
        IOController().remove_by_id(self.TYPE, self.id)

    def update_in_store(self):
        IOController().update(self)

    @classmethod
    def from_store(cls, id):
        dct = IOController().retrieve_by_id(cls.TYPE, id)
        return cls(store=False, **dct)


class NamedEntity(Entity):
    REQUIRED_ARGS = ['name']

    def __init__(self, store=True, **kwargs):
        super().__init__(store=False, **kwargs)

        if store:
            self.store()

    @property
    def name(self):
        return self._attrs['name']


class AutoFillEntity(Entity):
    """
    Abstract class with the capability to perform auto-fills
    """

    AUTO_ARGS = []
    TYPE = 'abstract'

    def __init__(self, store=True, **kwargs):
        super().__init__(store=False, **kwargs)

        # auto fill missing elements
        for elem in self.AUTO_ARGS:
            if kwargs.get(elem, None) is None:
                self._attrs.update({elem: self._auto_fill(elem)})

        if store:
            self.store()

    def _auto_fill(self, elem):
        return None


class RelationalEntity(AutoFillEntity):
    """
    Entities that can be in a one-to-many relationship with other entities
    """

    RELATIONSHIP_ATTR = 'relationships'
    RELATIONSHIP_TYPE = 'abstract'
    AUTO_ARGS = ['relationships']
    REQUIRED_ARGS = ['name']

    def __init__(self, store=True, **kwargs):
        super().__init__(store=False, **kwargs)

        self._relationship_list = []
        if store:
            self.store()

        # init the relationships
        self.add_relationship(self._attrs[self.RELATIONSHIP_ATTR])

    def _add_entity_by_id(self, entity_id):
        if not IOController().is_type(self.RELATIONSHIP_TYPE, entity_id):
            raise TypeError('Cannot establish a relationship with this entity, expected {}, but received something else'.format(self.RELATIONSHIP_TYPE))
        self._relationship_list.append(entity_id)

    def _add_entity(self, entity, establish_connection=True):
        if not issubclass(type(entity), Entity):
            raise TypeError('Argument has type {}, not Entity'.format(type(entity)))
        self._add_entity_by_id(entity.id)
        if establish_connection:
            self._establish_connection(entity)

    def _add_entities_by_ids(self, entity_ids):
        try:
            assert type(entity_ids) is list
        except AssertionError:
            raise TypeError('Argument is not a list, but {}'.format(type(entity_ids)))

        for entity_id in entity_ids:
            self._add_entity_by_id(entity_id)

    def _add_entities(self, entities, establish_connection=True):
        try:
            assert type(entities) is list
        except AssertionError:
            raise TypeError('Argument is not a list, but {}'.format(type(entities)))

        for entity in entities:
            self._add_entity(entity, establish_connection=establish_connection)

    def _establish_connection(self, entity):
        pass

    def _remove_connection(self, entity):
        pass

    @property
    def name(self):
        return self._attrs['name']

    def add_relationship(self, relationship, establish_connection=True):

        # entities relationships are stored with ids
        # we therefore allow for lists of entities, lists of ids, single entities and single ids
        if not relationship:
            return

        elif type(relationship) is list:
            try:
                self._add_entities(relationship, establish_connection=establish_connection)
            except TypeError:
                self._add_entities_by_ids(relationship)
        else:
            try:
                self._add_entity(relationship, establish_connection=establish_connection)
            except TypeError:
                self._add_entity_by_id(relationship)

        # update in store
        self._attrs.update({self.RELATIONSHIP_ATTR: self._relationship_list})
        self.update_in_store()

    def remove_relationship(self, relationship, remove_connection=True):
        try:
            assert relationship.id in self._relationship_list
        except AssertionError:
            raise ValueError('Relationship with {} does not exist'.format(relationship))

        # remove the relationship and the connection (i.e. the relationship in the other direction)
        self._relationship_list.remove(relationship.id)
        if remove_connection:
            self._remove_connection(relationship)

        self._attrs.update({self.RELATIONSHIP_ATTR: self._relationship_list})
        self.update_in_store()

    @property
    def relationships(self):
        return self._relationship_list


    def _check_args(self, **kwargs):
        """
        Checks whether the arguments are in the required structure
        :param kwargs: arguments
        :return: None
        """

        for arg in kwargs.keys():
            if arg == self.RELATIONSHIP_ATTR and (kwargs[arg] is None or type(kwargs[arg]) is list):
                continue
            if type(kwargs[arg]) not in self.ALLOWED_TYPES:
                raise TypeError('Argument {} has invalid type {}'.format(arg, type(kwargs[arg])))

    def to_df(self):

        dct = {attr: [self._attrs[attr]] for attr in self._attrs.keys()}

        # separate items in the relatioship list with a comma
        relationship_repr = ''
        for rel in self._relationship_list:
            relationship_repr += rel + ';'
        relationship_repr = relationship_repr[:-1]

        # update in dictionary
        dct.update({self.RELATIONSHIP_ATTR: [relationship_repr]})
        return pd.DataFrame(dct)

    @classmethod
    def from_store(cls, id):
        dct = IOController().retrieve_by_id(cls.TYPE, id)

        # check that the group is not nan
        no_relationships = False
        try:
            no_relationships = np.isnan(dct[cls.RELATIONSHIP_ATTR])
        except TypeError:
            pass

        if no_relationships:
            dct.update({cls.RELATIONSHIP_ATTR: None})
        else:
            lst = dct[cls.RELATIONSHIP_ATTR].split(';')
            dct.update({cls.RELATIONSHIP_ATTR: lst})

        return cls(store=False, **dct)


class Person(RelationalEntity):
    """
    Person class
    """

    RELATIONSHIP_ATTR = 'groups'
    RELATIONSHIP_TYPE = 'group'
    AUTO_ARGS = ['groups']
    TYPE = 'person'

    def __init__(self, store=True, **kwargs):
        super().__init__(store=store, **kwargs)

    @property
    def groups(self):
        return self._relationship_list

    def _establish_connection(self, entity):
        entity.add_member(self, establish_connection=False)

    def _remove_connection(self, entity):
        entity.remove_member(self, remove_connection=False)

    def add_to_group(self, group, establish_connection=True):
        self.add_relationship(group, establish_connection=establish_connection)

    def remove_from_group(self, group, remove_connection=True):
        self.remove_relationship(group, remove_connection=remove_connection)

    def __eq__(self, other):
        return type(other) is Person and self.id == other.id

    def refresh(self):
        # get the current version of the entity from the store
        p = Person.from_store(self.id)
        self._relationship_list = p.relationships
        self._attrs = p.attrs

    def make_payment(self, group, amount, **kwargs):
        _ = Payment(store=True, group_id=group.id, payer_id=self.id, amount=amount, **kwargs)


class Group(RelationalEntity):
    """
    Group class
    """

    RELATIONSHIP_ATTR = 'members'
    RELATIONSHIP_TYPE = 'person'
    AUTO_ARGS = ['members']
    TYPE = 'group'

    def __init__(self, store=True, **kwargs):
        super().__init__(store=store, **kwargs)

    @property
    def members(self):
        return self._relationship_list

    @property
    def payments(self):
        return [Payment(store=False, **dct) for dct in IOController().retrieve_payments_data_for_group(self.id)]

    def _establish_connection(self, entity):
        entity.add_to_group(self, establish_connection=False)

    def _remove_connection(self, entity):
        entity.remove_from_group(self, remove_connection=False)

    def add_member(self, person, establish_connection=True):
        self.add_relationship(person, establish_connection=establish_connection)

    def remove_member(self, person, remove_connection=True):
        self.remove_relationship(person, remove_connection=remove_connection)

    def __eq__(self, other):
        return type(other) is Group and self.id == other.id

    def refresh(self):
        # get the current version of the entity from the store
        g = Group.from_store(self.id)
        self._relationship_list = g.relationships
        self._attrs = g.attrs

    def register_payment(self, payer, amount, **kwargs):
        _ = Payment(store=True, group_id=self.id, payer_id=payer.id, amount=amount, **kwargs)

    def summarize_payments(self):

        # init the dataframe
        df = pd.DataFrame(index=self.members, columns=self.members).fillna(0.)

        # add the payments to it
        for p in self.payments:
            df += p.to_matrix()

        return df

    def remove_payment(self, payment):
        if type(payment) is Payment:
            payment.delete_from_store()
        elif type(payment) is str:
            IOController().remove_by_id('payment', payment)


class Payment(RelationalEntity):
    """
    Payment class
    """

    REQUIRED_ARGS = ['payer_id', 'group_id', 'amount']
    RELATIONSHIP_ATTR = 'paid_for'
    RELATIONSHIP_TYPE = 'person'
    AUTO_ARGS = ['paid_for', 'currency', 'purpose', 'comment', 'location']
    ALLOWED_RELATIONSHIP = Person
    TYPE = 'payment'

    def __init__(self, store=True, **kwargs):
        super().__init__(store=store, **kwargs)

    @property
    def group_id(self):
        return self._attrs['group_id']

    @property
    def payer_id(self):
        return self._attrs['payer_id']

    @property
    def amount(self):
        return self._attrs['amount']

    @property
    def currency(self):
        return self._attrs['currency']

    @property
    def purpose(self):
        return self._attrs['purpose']

    @property
    def comment(self):
        return self._attrs['comment']

    @property
    def location(self):
        return self._attrs['location']

    @property
    def paid_for(self):
        return self._attrs['paid_for']

    def _auto_fill(self, elem):
        # To be updated with a better autofill function
        if elem == 'paid_for':
            return Group.from_store(self._attrs['group_id']).members
        else:
            return {
                'currency': 'AUD',
                'purpose': 'That other thing, remember?',
                'comment': 'You better pay me back soon',
                'location': 'That place where we were'
            }[elem]

    def __eq__(self, other):
        return type(other) is Payment and self.id == other.id

    def add_person(self, person):
        self.add_relationship(person)

    def remove_person(self, person):
        self.remove_relationship(person)

    def to_matrix(self):
        
        # get the corresponding group
        g = Group.from_store(self.group_id)
        
        # create an empty dataframe
        df = pd.DataFrame(index=g.members, columns=g.members).fillna(0.)
        
        # add the payment to the dataframe
        for rec in self.paid_for:
            df.set_value(self.payer_id, rec, self.amount / len(self.paid_for))

        return df


