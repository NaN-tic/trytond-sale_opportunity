#This file is part of Tryton.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
from functools import wraps

from trytond.model import fields
from trytond.pool import Pool, PoolMeta
from trytond.transaction import Transaction
from trytond.pyson import Eval

__all__ = ['Sale', 'SaleLine']
__metaclass__ = PoolMeta


def process_opportunity(func):
    @wraps(func)
    def wrapper(cls, sales):
        pool = Pool()
        Opportunity = pool.get('sale.opportunity')
        func(cls, sales)
        with Transaction().set_user(0, set_context=True):
            Opportunity.process([o for s in cls.browse(sales)
                for o in s.opportunities])
    return wrapper


class Sale:
    __name__ = 'sale.sale'
    opportunities = fields.Many2Many('sale.opportunity-sale.sale',
            'sale', 'opportunity', 'Opportunities')

    @classmethod
    def __setup__(cls):
        super(Sale, cls).__setup__()
        cls._error_messages.update({
                'delete_last_sale': ('You cannot delete sale "%s" because it '
                    'is the last sale of opportunity "%s".'),
                })

    @classmethod
    def _get_origin(cls):
        return super(Sale, cls)._get_origin() + ['sale.opportunity']

    @classmethod
    def delete(cls, sales):
        for sale in sales:
            if any(len(o.sales) == 1 for o in sale.opportunities):
                cls.raise_user_error('delete_last_sale',
                        (sale.rec_name, sale.origin.rec_name))
        super(Sale, cls).delete(sales)

    @classmethod
    @process_opportunity
    def cancel(cls, sales):
        super(Sale, cls).cancel(sales)

    @classmethod
    @process_opportunity
    def draft(cls, sales):
        super(Sale, cls).draft(sales)

    @classmethod
    @process_opportunity
    def confirm(cls, sales):
        super(Sale, cls).confirm(sales)

    @classmethod
    @process_opportunity
    def process(cls, sales):
        super(Sale, cls).process(sales)


class SaleLine:
    __name__ = 'sale.line'

    origin = fields.Reference('Origin', selection='get_origin', select=True,
        states={
            'readonly': Eval('_parent_invoice', {}).get('state') != 'draft',
            })

    @classmethod
    def get_origin(cls):
        Model = Pool().get('ir.model')
        models = cls._get_origin()
        models = Model.search([
                ('model', 'in', models),
                ])
        return [(None, '')] + [(m.model, m.name) for m in models]

    @classmethod
    def _get_origin(cls):
        return ['sale.opportunity.line']
