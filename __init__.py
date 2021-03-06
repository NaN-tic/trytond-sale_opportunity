#This file is part of Tryton.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

from trytond.pool import Pool
from .sale import *
from .opportunity import *
from .configuration import *


def register():
    Pool.register(
        Sale,
        SaleLine,
        SaleOpportunity,
        OpportunitySale,
        SaleOpportunityLine,
        SaleOpportunityHistory,
        SaleOpportunityEmployee,
        OpenSaleOpportunityEmployeeStart,
        SaleOpportunityMonthly,
        SaleOpportunityEmployeeMonthly,
        Configuration,
        module='sale_opportunity', type_='model')
    Pool.register(
        ConvertOpportunity,
        OpenSaleOpportunityEmployee,
        module='sale_opportunity', type_='wizard')
