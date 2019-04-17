# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
"Sales extension for managing leads and opportunities"
import datetime
import time
from sql import Column, Literal
from sql.aggregate import Min, Max, Count, Sum
from sql.conditionals import Coalesce, Case
from sql.functions import Extract

from trytond.model import ModelView, ModelSQL, Workflow, Check, fields
from trytond.wizard import Wizard, StateView, StateAction, Button
from trytond import backend
from trytond.pyson import Equal, Eval, Not, In, If, Get, PYSONEncoder
from trytond.transaction import Transaction
from trytond.pool import Pool

__all__ = ['SaleOpportunity', 'OpportunitySale', 'SaleOpportunityLine',
    'SaleOpportunityHistory', 'ConvertOpportunity', 'SaleOpportunityEmployee',
    'OpenSaleOpportunityEmployeeStart', 'OpenSaleOpportunityEmployee',
    'SaleOpportunityMonthly', 'SaleOpportunityEmployeeMonthly']

STATES = [
    ('lead', 'Lead'),
    ('opportunity', 'Opportunity'),
    ('converted', 'Converted into Sale'),
    ('won', 'Won'),
    ('cancelled', 'Cancelled'),
    ('lost', 'Lost'),
]
_STATES_START = {
    'readonly': Eval('state') != 'lead',
    }
_DEPENDS_START = ['state']
_STATES_STOP = {
    'readonly': In(Eval('state'), ['won', 'lost', 'cancelled']),
}
_DEPENDS_STOP = ['state']


class SaleOpportunity(Workflow, ModelSQL, ModelView):
    'Sale Opportunity'
    __name__ = "sale.opportunity"
    _history = True
    _rec_name = 'reference'
    reference = fields.Char('Reference', readonly=True, required=True,
        select=True)
    party = fields.Many2One('party.party', 'Party', select=True,
        states={
            'readonly': Eval('state').in_(['converted', 'lost', 'cancelled']),
            'required': ~Eval('state').in_(['lead', 'lost', 'cancelled']),
            }, depends=['state'])
    address = fields.Many2One('party.address', 'Address',
        domain=[('party', '=', Eval('party'))],
        select=True, depends=['party', 'state'],
        states=_STATES_STOP)
    company = fields.Many2One('company.company', 'Company', required=True,
        select=True, states=_STATES_STOP, domain=[
            ('id', If(In('company', Eval('context', {})), '=', '!='),
                Get(Eval('context', {}), 'company', 0)),
            ], depends=_DEPENDS_STOP)
    currency = fields.Function(fields.Many2One('currency.currency',
        'Currency'), 'get_currency')
    currency_digits = fields.Function(fields.Integer('Currency Digits'),
            'get_currency_digits')
    expected_amount = fields.Numeric('Expected Amount',
        digits=(16, Eval('currency_digits', 2)),
        states=_STATES_STOP, depends=_DEPENDS_STOP + ['currency_digits'],
        help='Estimated revenue amount')
    won_amount = fields.Numeric('Won Amount', readonly=True, states={
                'invisible': Eval('state') != 'won',
            }, digits=(16, Eval('currency_digits', 2)),
            depends=['state', 'currency_digits'])
    payment_term = fields.Many2One('account.invoice.payment_term',
        'Payment Term', states={
            'required': Eval('state') == 'converted',
            'readonly': In(Eval('state'),
                ['converted', 'won', 'lost', 'cancelled']),
            },
        depends=['state'])
    employee = fields.Many2One('company.employee', 'Employee', required=True,
            states=_STATES_STOP, depends=['state', 'company'],
            domain=[('company', '=', Eval('company'))])
    start_date = fields.Date('Start Date', required=True, select=True,
        states=_STATES_START, depends=_DEPENDS_START)
    end_date = fields.Date('End Date', select=True, readonly=True, states={
        'invisible': Not(In(Eval('state'),
            ['converted', 'won', 'cancelled', 'lost'])),
    }, depends=['state'])
    description = fields.Char('Description', required=True,
        states=_STATES_STOP, depends=_DEPENDS_STOP)
    comment = fields.Text('Comment', states=_STATES_STOP,
        depends=_DEPENDS_STOP)
    lines = fields.One2Many('sale.opportunity.line', 'opportunity', 'Lines',
        states=_STATES_STOP, depends=_DEPENDS_STOP)
    state = fields.Selection(STATES, 'State', required=True, select=True,
            sort=False, readonly=True)
    probability = fields.Integer('Conversion Probability', required=True,
            states={
                'readonly': In(Eval('state'), ['won', 'lost', 'cancelled']),
            }, depends=['state'], help="Percentage between 0 and 100")
    history = fields.One2Many('sale.opportunity.history', 'opportunity',
            'History', readonly=True)
    lost_reason = fields.Text('Reason for loss', states={
            'invisible': Eval('state') != 'lost',
            }, depends=['state'])
    sale_state = fields.Selection([
        ('none', 'None'),
        ('waiting', 'Waiting'),
        ('confirmed', 'Confirmed'),
        ('done', 'Done'),
        ('cancelled', 'Cancelled'),
        ], 'Sale State', readonly=True, required=True)
    sales = fields.Many2Many('sale.opportunity-sale.sale',
            'opportunity', 'sale', 'Sales', readonly=True)

    @classmethod
    def __register__(cls, module_name):
        cursor = Transaction().connection.cursor()
        TableHandler = backend.get('TableHandler')
        sql_table = cls.__table__()

        reference_exists = True
        if TableHandler.table_exist(cls._table):
            table = TableHandler(cls, module_name)
            reference_exists = table.column_exist('reference')

        table = TableHandler(cls, module_name)
        # Migration from 2.8: amount renamed into expected_amount
        if table.column_exist('amount'):
            table.column_rename('amount', 'expected_amount')

        super(SaleOpportunity, cls).__register__(module_name)
        table = TableHandler(cls, module_name)

        # Migration from 2.8: make party not required and add reference as
        # required
        table.not_null_action('party', action='remove')
        if not reference_exists:
            cursor.execute(*sql_table.update(
                    columns=[sql_table.reference],
                    values=[sql_table.id],
                    where=sql_table.reference == None))
            table.not_null_action('reference', action='add')

    @classmethod
    def __setup__(cls):
        super(SaleOpportunity, cls).__setup__()
        cls._order.insert(0, ('start_date', 'DESC'))
        t = cls.__table__()
        cls._sql_constraints += [
            ('check_percentage',
                Check(t, ((t.probability >= 0) & (t.probability <= 100))),
                'Probability must be between 0 and 100.')
            ]
        cls._error_messages.update({
                'delete_cancel': ('Sale Opportunity "%s" must be cancelled '
                    'before deletion.'),
                })
        cls._transitions |= set((
                ('lead', 'opportunity'),
                ('lead', 'lost'),
                ('lead', 'cancelled'),
                ('opportunity', 'converted'),
                ('opportunity', 'lead'),
                ('opportunity', 'lost'),
                ('opportunity', 'won'),
                ('opportunity', 'cancelled'),
                ('converted', 'converted'),
                ('converted', 'won'),
                ('converted', 'lost'),
                ('won', 'opportunity'),
                ('lost', 'lead'),
                ('cancelled', 'lead'),
                ))
        cls._buttons.update({
                'lead': {
                    'invisible': ~Eval('state').in_(
                        ['cancelled', 'lost', 'opportunity']),
                    'icon': If(Eval('state').in_(['cancelled', 'lost']),
                        'tryton-clear', 'tryton-go-previous'),
                    },
                'opportunity': {
                    'invisible': ~Eval('state').in_(['lead', 'won']),
                    'icon': If(Eval('state') == 'won',
                        'tryton-go-previous', 'tryton-go-next'),
                    },
                'convert': {
                    'invisible': ~Eval('state').in_(
                        ['opportunity', 'converted']),
                    },
                'lost': {
                    'invisible': ~Eval('state').in_(['lead', 'opportunity']),
                    },
                'cancel': {
                    'invisible': ~Eval('state').in_(['lead', 'opportunity']),
                    },
                'won': {
                    'invisible': (~Eval('state').in_(['opportunity']))
                    },
                })

    @staticmethod
    def default_state():
        return 'lead'

    @staticmethod
    def default_start_date():
        Date = Pool().get('ir.date')
        return Date.today()

    @staticmethod
    def default_probability():
        return 50

    @staticmethod
    def default_company():
        return Transaction().context.get('company')

    @staticmethod
    def default_employee():
        User = Pool().get('res.user')

        if Transaction().context.get('employee'):
            return Transaction().context['employee']
        else:
            user = User(Transaction().user)
            if user.employee:
                return user.employee.id

    @classmethod
    def default_payment_term(cls):
        PaymentTerm = Pool().get('account.invoice.payment_term')
        payment_terms = PaymentTerm.search(cls.payment_term.domain)
        if len(payment_terms) == 1:
            return payment_terms[0].id

    @staticmethod
    def default_sale_state():
        return 'none'

    @classmethod
    def create(cls, vlist):
        pool = Pool()
        Sequence = pool.get('ir.sequence')
        Config = pool.get('sale.configuration')

        sequence = Config(1).sale_opportunity_sequence
        vlist = [x.copy() for x in vlist]
        for vals in vlist:
            vals['reference'] = Sequence.get_id(sequence.id)
        return super(SaleOpportunity, cls).create(vlist)

    @classmethod
    def copy(cls, opportunities, default=None):
        if default is None:
            default = {}
        default = default.copy()
        default.setdefault('sale_state', 'none')
        default.setdefault('won_amount', None)
        default.setdefault('reference', None)
        default.setdefault('history', None)
        default.setdefault('sales', None)
        return super(SaleOpportunity, cls).copy(opportunities, default=default)

    def get_currency(self, name):
        return self.company.currency.id

    def get_currency_digits(self, name):
        return self.company.currency.digits

    @fields.depends('company')
    def on_change_company(self):
        if self.company:
            self.currency = self.company.currency.id
            self.currency_digits = self.company.currency.digits

    @fields.depends('party')
    def on_change_party(self):
        self.payment_term = None
        if self.party:
            if self.party.customer_payment_term:
                self.payment_term = self.party.customer_payment_term.id
        if not self.payment_term:
            self.payment_term = self.default_payment_term()

    def _get_sale_line_opportunity_line(self, sale):
        '''
        Return sale lines for each opportunity line
        '''
        res = {}
        for line in self.lines:
            sale_line = line.get_sale_line(sale)
            if sale_line:
                res[line] = sale_line
        return res

    def _get_sale_opportunity(self):
        '''
        Return sale for an opportunity
        '''
        Sale = Pool().get('sale.sale')
        sale = Sale(
            description=self.description,
            party=self.party,
            payment_term=self.payment_term,
            company=self.company,
            invoice_address=self.address,
            shipment_address=self.address,
            currency=self.company.currency,
            sale_date=None,
            )
        if hasattr(self.party, 'customer_payment_type'):
            sale.payment_type = self.party.customer_payment_type
        return sale

    def create_sale(self):
        '''
        Create a sale for the opportunity and return the sale
        '''
        sale = self._get_sale_opportunity()
        sale_lines = self._get_sale_line_opportunity_line(sale)
        sale.save()

        for opportunity_line, sale_line in sale_lines.iteritems():
            sale_line.sale = sale
            sale_line.origin = opportunity_line
            sale_line.save()

        self.write([self], {
                'sales': [('add', [sale.id])],
                })
        return sale

    def is_reset(self):
        return self.state == 'lost' and self.sale_state == 'waiting'

    def is_won(self):
        return self.sale_state == 'confirmed'

    def is_lost(self):
        return self.sale_state == 'cancelled'

    def get_sale_state(self):
        '''
        Return the sale state for the opportunity.
        '''
        if not self.sales:
            return 'none'
        if all(s.state == 'done' for s in self.sales):
            return 'done'
        elif any(s.state in ['confirmed', 'processing', 'done']
                for s in self.sales):
            return 'confirmed'
        elif all(s.state == 'cancel' for s in self.sales):
            return 'cancelled'
        elif any(s.state == 'draft' for s in self.sales):
            return 'waiting'
        return 'none'

    def set_sale_state(self):
        '''
        Set the sale state.
        '''
        state = self.get_sale_state()
        if self.sale_state != state:
            self.write([self], {
                    'sale_state': state,
                    })

    @classmethod
    def set_end_date(cls, opportunities):
        Date = Pool().get('ir.date')
        cls.write(opportunities, {
                'end_date': Date.today(),
                })

    @classmethod
    def set_won_amount(cls, opportunities):
        for opportunity in opportunities:
            won_amount = sum(s.total_amount for s in opportunity.sales
                    if s.state in ['confirmed', 'processing', 'done'])
            cls.write([opportunity], {
                'won_amount': won_amount,
                })

    @classmethod
    def delete(cls, opportunities):
        # Cancel before delete
        cls.cancel(opportunities)
        for opportunity in opportunities:
            if opportunity.state != 'cancelled':
                cls.raise_user_error('delete_cancel', opportunity.rec_name)
        super(SaleOpportunity, cls).delete(opportunities)

    @classmethod
    def process(cls, opportunities):
        won = []
        lost = []
        reset = []
        for opportunity in opportunities:
            opportunity.set_sale_state()
            if opportunity.is_won():
                won.append(opportunity)
            elif opportunity.is_lost():
                lost.append(opportunity)
            elif opportunity.is_reset():
                reset.append(opportunity)
        if won:
            cls.won(won)
            cls.write(won, {
                'state': 'won',
                })
        if lost:
            cls.lost(lost)
            cls.write(lost, {
                'state': 'lost',
                })
        if reset:
            cls.write(lost, {
                'state': 'converted',
                })

    @classmethod
    @ModelView.button
    @Workflow.transition('lead')
    def lead(cls, opportunities):
        pass

    @classmethod
    @ModelView.button
    @Workflow.transition('opportunity')
    def opportunity(cls, opportunities):
        pass

    @classmethod
    @ModelView.button_action('sale_opportunity.wizard_convert')
    def convert(cls, opportunities):
        pass

    @classmethod
    @Workflow.transition('converted')
    def _convert(cls, opportunities):
        sales = []
        for opportunity in opportunities:
            sale = opportunity.create_sale()
            opportunity.set_sale_state()
            sales.append(sale)
        return sales

    @classmethod
    @ModelView.button
    @Workflow.transition('won')
    def won(cls, opportunities):
        cls.set_end_date(opportunities)
        cls.set_won_amount(opportunities)

    @classmethod
    @ModelView.button
    @Workflow.transition('lost')
    def lost(cls, opportunities):
        cls.set_end_date(opportunities)

    @classmethod
    @ModelView.button
    @Workflow.transition('cancelled')
    def cancel(cls, opportunities):
        cls.set_end_date(opportunities)


class OpportunitySale(ModelSQL):
    'Opportunity - Sale'
    __name__ = 'sale.opportunity-sale.sale'
    _table = 'opportunity_sale_rel'

    opportunity = fields.Many2One('sale.opportunity', 'Opportunity',
            ondelete='CASCADE', select=True, required=True)
    sale = fields.Many2One('sale.sale', 'Sale',
            ondelete='RESTRICT', select=True, required=True)

    @classmethod
    def __register__(cls, module_name):
        super(OpportunitySale, cls).__register__(module_name)
        Opportunity = Pool().get('sale.opportunity')
        TableHandler = backend.get('TableHandler')
        cursor = Transaction().connection.cursor()
        sql_table = cls.__table__()
        opportunity = Opportunity.__table__()

        # Migration from 2.8: convert Many2One field 'sale' to Many2Many
        # field 'sales'
        opportunity_table = TableHandler(Opportunity, module_name)
        if opportunity_table.column_exist('sale'):
            cursor.execute(*opportunity.select(
                opportunity.id, opportunity.sale,
                where=opportunity.sale != None))
            opportunity_ids = []
            for opportunity_id, sale_id in cursor.fetchall():
                cursor.execute(*sql_table.insert(
                    columns=[sql_table.opportunity, sql_table.sale],
                    values=[[opportunity_id, sale_id]]))
                opportunity_ids.append(opportunity_id)
            Opportunity.process(Opportunity.browse(opportunity_ids))
            opportunity_table.drop_column('sale', exception=True)

    @classmethod
    def delete(cls, opportunity_sales):
        opportunities = [x.opportunity for x in opportunity_sales]
        super(OpportunitySale, cls).delete(opportunity_sales)
        for op in opportunities:
            op.set_sale_state()

    @classmethod
    def create(cls, vlist):
        res = super(OpportunitySale, cls).create(vlist)
        for r in res:
            r.opportunity.set_sale_state()


class SaleOpportunityLine(ModelSQL, ModelView):
    'Sale Opportunity Line'
    __name__ = "sale.opportunity.line"
    _rec_name = "product"
    _history = True
    opportunity = fields.Many2One('sale.opportunity', 'Opportunity')
    sequence = fields.Integer('Sequence')
    product = fields.Many2One('product.product', 'Product', required=True,
            domain=[('salable', '=', True)])
    quantity = fields.Float('Quantity', required=True,
            digits=(16, Eval('unit_digits', 2)), depends=['unit_digits'])
    unit = fields.Many2One('product.uom', 'Unit', required=True)
    unit_digits = fields.Function(fields.Integer('Unit Digits'),
        'on_change_with_unit_digits')
    sale_lines = fields.One2Many('sale.line', 'origin', 'Sale Lines',
            readonly=True)

    @classmethod
    def __setup__(cls):
        super(SaleOpportunityLine, cls).__setup__()
        cls._order.insert(0, ('sequence', 'ASC'))

    @classmethod
    def __register__(cls, module_name):
        SaleLine = Pool().get('sale.line')
        TableHandler = backend.get('TableHandler')
        cursor = Transaction().connection.cursor()
        table = TableHandler(cls, module_name)
        sql_table = cls.__table__()
        sale_line = SaleLine.__table__()

        super(SaleOpportunityLine, cls).__register__(module_name)

        # Migration from 2.4: drop required on sequence
        table.not_null_action('sequence', action='remove')

        # Migration from 2.8: convert Many2One field 'sale_line' to One2Many
        # field 'sale_lines'
        if table.column_exist('sale_line'):
            cursor.execute(*sql_table.select(sql_table.id, sql_table.sale_line,
                where=sql_table.sale_line != None))
            for opportunity_line_id, sale_line_id in cursor.fetchall():
                cursor.execute(*sale_line.update(columns=[sale_line.origin],
                    values=['sale.opportunity.line,' + opportunity_line_id],
                    where=sale_line.id == sale_line_id))
            table.drop_column('sale_line', exception=True)

    @classmethod
    def copy(cls, lines, default=None):
        if default is None:
            default = {}
        default = default.copy()
        default['sale_lines'] = []
        res = super(SaleOpportunityLine, cls).copy(lines, default=default)
        return res

    @staticmethod
    def order_sequence(tables):
        table, _ = tables[None]
        return [table.sequence == None, table.sequence]

    @fields.depends('unit')
    def on_change_with_unit_digits(self, name=None):
        if self.unit:
            return self.unit.digits
        return 2

    @fields.depends('product', 'unit')
    def on_change_product(self):
        if not self.product:
            return

        category = self.product.sale_uom.category
        if not self.unit or self.unit not in category.uoms:
            self.unit = self.product.sale_uom.id
            self.unit_digits = self.product.sale_uom.digits

    def get_sale_line(self, sale):
        '''
        Return sale line for opportunity line
        '''
        SaleLine = Pool().get('sale.line')
        sale_line = SaleLine(
            type='line',
            quantity=self.quantity,
            unit=self.unit,
            product=self.product,
            sale=sale,
            description=None,
            )
        sale_line.on_change_product()
        return sale_line


class SaleOpportunityHistory(ModelSQL, ModelView):
    'Sale Opportunity History'
    __name__ = 'sale.opportunity.history'

    date = fields.DateTime('Change Date')
    opportunity = fields.Many2One('sale.opportunity', 'Sale Opportunity')
    user = fields.Many2One('res.user', 'User')
    party = fields.Many2One('party.party', 'Party', datetime_field='date')
    address = fields.Many2One('party.address', 'Address',
            datetime_field='date')
    company = fields.Many2One('company.company', 'Company',
            datetime_field='date')
    employee = fields.Many2One('company.employee', 'Employee',
            datetime_field='date')
    start_date = fields.Date('Start Date')
    end_date = fields.Date('End Date', states={
        'invisible': Not(In(Eval('state'),
            ['converted', 'cancelled', 'lost'])),
    }, depends=['state'])
    description = fields.Char('Description')
    comment = fields.Text('Comment')
    lines = fields.Function(fields.One2Many('sale.opportunity.line', None,
            'Lines', datetime_field='date'), 'get_lines')
    state = fields.Selection(STATES, 'State')
    probability = fields.Integer('Conversion Probability')
    lost_reason = fields.Text('Reason for loss', states={
        'invisible': Not(Equal(Eval('state'), 'lost')),
    }, depends=['state'])

    @classmethod
    def __setup__(cls):
        super(SaleOpportunityHistory, cls).__setup__()
        cls._order.insert(0, ('date', 'DESC'))

    @classmethod
    def table_query(cls):
        Opportunity = Pool().get('sale.opportunity')
        opportunity_history = Opportunity.__table_history__()
        columns = [
            Max(Column(opportunity_history, '__id')).as_('id'),
            opportunity_history.id.as_('opportunity'),
            Min(Coalesce(opportunity_history.write_date,
                    opportunity_history.create_date)).as_('date'),
            Coalesce(opportunity_history.write_uid,
                opportunity_history.create_uid).as_('user'),
            ]
        group_by = [
            opportunity_history.id,
            Coalesce(opportunity_history.write_uid,
                opportunity_history.create_uid),
            ]
        for name, field in cls._fields.iteritems():
            if name in ('id', 'opportunity', 'date', 'user', 'rec_name'):
                continue
            if not field.sql_type():
                continue
            column = Column(opportunity_history, name)
            columns.append(column.as_(name))
            group_by.append(column)

        where = Column(opportunity_history, '__id').in_(
            opportunity_history.select(Max(Column(opportunity_history,
                        '__id')),
                group_by=(opportunity_history.id,
                    Coalesce(opportunity_history.write_date,
                    opportunity_history.create_date))))

        return opportunity_history.select(*columns, where=where,
            group_by=group_by)

    def get_lines(self, name):
        Line = Pool().get('sale.opportunity.line')
        # We will always have only one id per call due to datetime_field
        lines = Line.search([
                ('opportunity', '=', self.opportunity.id),
                ])
        return [l.id for l in lines]

    @classmethod
    def read(cls, ids, fields_names=None):
        res = super(SaleOpportunityHistory, cls).read(ids,
            fields_names=fields_names)

        # Remove microsecond from timestamp
        for values in res:
            if 'date' in values:
                if isinstance(values['date'], basestring):
                    values['date'] = datetime.datetime(
                        *time.strptime(values['date'],
                            '%Y-%m-%d %H:%M:%S.%f')[:6])
                values['date'] = values['date'].replace(microsecond=0)
        return res


class ConvertOpportunity(Wizard):
    'Convert Opportunity'
    __name__ = 'sale.opportunity.convert_opportunity'
    start_state = 'convert'
    convert = StateAction('sale.act_sale_form')

    def do_convert(self, action):
        pool = Pool()
        Opportunity = pool.get('sale.opportunity')
        opportunities = Opportunity.browse(Transaction().context['active_ids'])
        sales = Opportunity._convert(opportunities)
        data = {'res_id': [s.id for s in sales]}
        if len(sales) == 1:
            action['views'].reverse()
        return action, data


class SaleOpportunityEmployee(ModelSQL, ModelView):
    'Sale Opportunity per Employee'
    __name__ = 'sale.opportunity_employee'

    employee = fields.Many2One('company.employee', 'Employee')
    number = fields.Integer('Number')
    won = fields.Integer('Won')
    win_rate = fields.Function(fields.Float('Win Rate',
        help='In %'), 'get_win_rate')
    lost = fields.Integer('Lost')
    company = fields.Many2One('company.company', 'Company')
    currency = fields.Function(fields.Many2One('currency.currency',
        'Currency'), 'get_currency')
    currency_digits = fields.Function(fields.Integer('Currency Digits'),
            'get_currency_digits')
    expected_amount = fields.Numeric('Expected Amount',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits'])
    won_amount = fields.Numeric('Won Amount',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits'])
    won_amount_rate = fields.Function(fields.Float(
        'Won Amount Rate', help='In %'), 'get_won_amount_rate')

    @staticmethod
    def _won_state():
        return ['won']

    @staticmethod
    def _lost_state():
        return ['lost']

    @classmethod
    def table_query(cls):
        Opportunity = Pool().get('sale.opportunity')
        opportunity = Opportunity.__table__()
        where = Literal(True)
        if Transaction().context.get('start_date'):
            where &= (opportunity.start_date >=
                Transaction().context['start_date'])
        if Transaction().context.get('end_date'):
            where &= (opportunity.start_date <=
                Transaction().context['end_date'])
        return opportunity.select(
            opportunity.employee.as_('id'),
            Max(opportunity.create_uid).as_('create_uid'),
            Max(opportunity.create_date).as_('create_date'),
            Max(opportunity.write_uid).as_('write_uid'),
            Max(opportunity.write_date).as_('write_date'),
            opportunity.employee,
            opportunity.company,
            Count(Literal(1)).as_('number'),
            Sum(Case((opportunity.state.in_(cls._won_state()),
                        Literal(1)), else_=Literal(0))).as_('won'),
            Sum(Case((opportunity.state.in_(cls._lost_state()),
                        Literal(1)), else_=Literal(0))).as_('lost'),
            Sum(opportunity.expected_amount).as_('expected_amount'),
            Sum(opportunity.won_amount).as_('won_amount'),
            where=where,
            group_by=(opportunity.employee, opportunity.company))

    def get_win_rate(self, name):
        if self.number:
            return float(self.won) / self.number * 100.0
        else:
            return 0.0

    def get_currency(self, name):
        return self.company.currency.id

    def get_currency_digits(self, name):
        return self.company.currency.digits

    def get_won_amount_rate(self, name):
        if self.expected_amount and self.won_amount:
            return float(self.won_amount) / float(self.expected_amount) * 100.0
        else:
            return 0.0


class OpenSaleOpportunityEmployeeStart(ModelView):
    'Open Sale Opportunity per Employee'
    __name__ = 'sale.opportunity_employee.open.start'
    start_date = fields.Date('Start Date')
    end_date = fields.Date('End Date')


class OpenSaleOpportunityEmployee(Wizard):
    'Open Sale Opportunity per Employee'
    __name__ = 'sale.opportunity_employee.open'
    start = StateView('sale.opportunity_employee.open.start',
        'sale_opportunity.opportunity_employee_open_start_view_form', [
            Button('Cancel', 'end', 'tryton-cancel'),
            Button('Open', 'open_', 'tryton-ok', default=True),
            ])
    open_ = StateAction('sale_opportunity.act_opportunity_employee_form')

    def do_open_(self, action):
        action['pyson_context'] = PYSONEncoder().encode({
                'start_date': self.start.start_date,
                'end_date': self.start.end_date,
                })
        return action, {}


class SaleOpportunityMonthly(ModelSQL, ModelView):
    'Sale Opportunity per Month'
    __name__ = 'sale.opportunity_monthly'
    year = fields.Char('Year')
    month = fields.Integer('Month')
    year_month = fields.Function(fields.Char('Year-Month'),
            'get_year_month')
    number = fields.Integer('Number')
    won = fields.Integer('Won')
    win_rate = fields.Function(fields.Float('Win Rate',
        help='In %'), 'get_win_rate')
    lost = fields.Integer('Lost')
    company = fields.Many2One('company.company', 'Company')
    currency = fields.Function(fields.Many2One('currency.currency',
        'Currency'), 'get_currency')
    currency_digits = fields.Function(fields.Integer('Currency Digits'),
            'get_currency_digits')
    expected_amount = fields.Numeric('Expected Amount',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits'])
    won_amount = fields.Numeric('Won Amount',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits'])
    won_amount_rate = fields.Function(fields.Float(
        'Won Amount Rate', help='In %'), 'get_won_amount_rate')

    @classmethod
    def __setup__(cls):
        super(SaleOpportunityMonthly, cls).__setup__()
        cls._order.insert(0, ('year', 'DESC'))
        cls._order.insert(1, ('month', 'DESC'))

    @staticmethod
    def _won_state():
        return ['won']

    @staticmethod
    def _lost_state():
        return ['lost']

    @classmethod
    def table_query(cls):
        Opportunity = Pool().get('sale.opportunity')
        opportunity = Opportunity.__table__()
        type_id = cls.id.sql_type().base
        type_year = cls.year.sql_type().base
        year_column = Extract('YEAR',
            opportunity.start_date).cast(type_year).as_('year')
        month_column = Extract('MONTH', opportunity.start_date).as_('month')
        return opportunity.select(
            Max(Extract('MONTH', opportunity.start_date)
                + Extract('YEAR', opportunity.start_date) * 100
                ).cast(type_id).as_('id'),
            Max(opportunity.create_uid).as_('create_uid'),
            Max(opportunity.create_date).as_('create_date'),
            Max(opportunity.write_uid).as_('write_uid'),
            Max(opportunity.write_date).as_('write_date'),
            year_column,
            month_column,
            opportunity.company,
            Count(Literal(1)).as_('number'),
            Sum(Case((opportunity.state.in_(cls._won_state()),
                        Literal(1)), else_=Literal(0))).as_('won'),
            Sum(Case((opportunity.state.in_(cls._lost_state()),
                        Literal(1)), else_=Literal(0))).as_('lost'),
            Sum(opportunity.expected_amount).as_('expected_amount'),
            Sum(Case((opportunity.state.in_(cls._won_state()),
                        opportunity.expected_amount),
                    else_=Literal(0))).as_('won_amount'),
            group_by=(year_column, month_column, opportunity.company))

    def get_win_rate(self, name):
        if self.number:
            return float(self.won) / self.number * 100.0
        else:
            return 0.0

    def get_year_month(self, name):
        return '%s-%s' % (self.year, int(self.month))

    def get_currency(self, name):
        return self.company.currency.id

    def get_currency_digits(self, name):
        return self.company.currency.digits

    def get_won_amount_rate(self, name):
        if self.expected_amount and self.won_amount:
            return float(self.won_amount) / float(self.expected_amount) * 100.0
        else:
            return 0.0


class SaleOpportunityEmployeeMonthly(ModelSQL, ModelView):
    'Sale Opportunity per Employee per Month'
    __name__ = 'sale.opportunity_employee_monthly'
    year = fields.Char('Year')
    month = fields.Integer('Month')
    employee = fields.Many2One('company.employee', 'Employee')
    number = fields.Integer('Number')
    won = fields.Integer('Won')
    win_rate = fields.Function(fields.Float('Win Rate',
        help='In %'), 'get_win_rate')
    lost = fields.Integer('Lost')
    company = fields.Many2One('company.company', 'Company')
    currency = fields.Function(fields.Many2One('currency.currency',
        'Currency'), 'get_currency')
    currency_digits = fields.Function(fields.Integer('Currency Digits'),
            'get_currency_digits')
    expected_amount = fields.Numeric('Expected Amount',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits'])
    won_amount = fields.Numeric('Won Amount',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits'])
    won_amount_rate = fields.Function(fields.Float(
        'Won Amount Rate', help='In %'), 'get_won_amount_rate')

    @classmethod
    def __setup__(cls):
        super(SaleOpportunityEmployeeMonthly, cls).__setup__()
        cls._order.insert(0, ('year', 'DESC'))
        cls._order.insert(1, ('month', 'DESC'))
        cls._order.insert(2, ('employee', 'ASC'))

    @staticmethod
    def _won_state():
        return ['won']

    @staticmethod
    def _lost_state():
        return ['lost']

    @classmethod
    def table_query(cls):
        Opportunity = Pool().get('sale.opportunity')
        opportunity = Opportunity.__table__()
        type_id = cls.id.sql_type().base
        type_year = cls.year.sql_type().base
        year_column = Extract('YEAR',
            opportunity.start_date).cast(type_year).as_('year')
        month_column = Extract('MONTH', opportunity.start_date).as_('month')
        return opportunity.select(
            Max(Extract('MONTH', opportunity.start_date)
                + Extract('YEAR', opportunity.start_date) * 100
                + opportunity.employee * 1000000
                ).cast(type_id).as_('id'),
            Max(opportunity.create_uid).as_('create_uid'),
            Max(opportunity.create_date).as_('create_date'),
            Max(opportunity.write_uid).as_('write_uid'),
            Max(opportunity.write_date).as_('write_date'),
            year_column,
            month_column,
            opportunity.employee,
            opportunity.company,
            Count(Literal(1)).as_('number'),
            Sum(Case((opportunity.state.in_(cls._won_state()),
                        Literal(1)), else_=Literal(0))).as_('won'),
            Sum(Case((opportunity.state.in_(cls._lost_state()),
                        Literal(1)), else_=Literal(0))).as_('lost'),
            Sum(opportunity.expected_amount).as_('expected_amount'),
            Sum(Case((opportunity.state.in_(cls._won_state()),
                        opportunity.expected_amount),
                    else_=Literal(0))).as_('won_amount'),
            group_by=(year_column, month_column, opportunity.employee,
                opportunity.company))

    def get_win_rate(self, name):
        if self.number:
            return float(self.won) / self.number * 100.0
        else:
            return 0.0

    def get_currency(self, name):
        return self.company.currency.id

    def get_currency_digits(self, name):
        return self.company.currency.digits

    def get_won_amount_rate(self, name):
        if self.expected_amount and self.won_amount:
            return float(self.won_amount) / float(self.expected_amount) * 100.0
        else:
            return 0.0
