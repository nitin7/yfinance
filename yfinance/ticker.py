#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import datetime as _datetime
import time
from collections import namedtuple as _namedtuple

import pandas as _pd
import requests
from requests.packages.urllib3.util.retry import Retry

from .base import TickerBase
from .utils import TimeoutHTTPAdapter

# Create robust request session object
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504, 404])
http = requests.Session()
http.mount("https://", TimeoutHTTPAdapter(max_retries=retries, timeout=30, pool_connections=100, pool_maxsize=100))


class TickerException(Exception):
    pass


class Ticker(TickerBase):

    def __repr__(self):
        return 'yfinance.Ticker object <%s>' % self.ticker

    def _download_options(self, date=None, proxy=None):

        if not self._options:
            # Setup proxy in requests format
            if proxy is not None:
                if isinstance(proxy, dict) and "https" in proxy:
                    proxy = proxy["https"]
                proxy = {"https": proxy}

            # Get options from api
            url = "{}/v7/finance/options/{}?getAllData=true".format(self._base_url, self.ticker.replace('.', '-'), date) # replace . in ticker with - to get data (ex. RDS.B, BRK.B)
            resp = http.get(url=url, proxies=proxy)
            r = resp.json()
            result = r['optionChain']['result'][0]
            if result:
                for exp in result['expirationDates']:
                    # Don't populate expirations greater than three years from now (ex. TWTR has 2026 expiries with no data)
                    if (exp - time.time()) > (3600 * 24 * 365 * 3):
                        continue
                    self._expirations[_datetime.datetime.fromtimestamp(exp).strftime('%Y-%m-%d')] = exp
                self._options = result['options']
            else:
                raise TickerException('Options response empty for ticker {}: {}'.format(self.ticker, r))

        if not self._options:
            raise TickerException('Options empty for ticker {}'.format(self.ticker))

        return self._options[0] if date is None else self._options[list(self._expirations.values()).index(date)]

    def _options2df(self, opt, tz=None):
        data = _pd.DataFrame(opt).reindex(columns=[
            'contractSymbol',
            'lastTradeDate',
            'strike',
            'lastPrice',
            'bid',
            'ask',
            'change',
            'percentChange',
            'volume',
            'openInterest',
            'impliedVolatility',
            'inTheMoney',
            'contractSize',
            'currency'])

        data['lastTradeDate'] = _pd.to_datetime(
            data['lastTradeDate'], unit='s')
        if tz is not None:
            data['lastTradeDate'] = data['lastTradeDate'].tz_localize(tz)
        return data

    def option_chain(self, date=None, proxy=None, tz=None):
        if date is None:
            options = self._download_options(proxy=proxy)
        else:
            if not self._expirations:
                self._download_options()
            if date not in self._expirations:
                raise ValueError(
                    "Expiration `%s` cannot be found. "
                    "Available expiration are: [%s]" % (
                        date, ', '.join(self._expirations)))
            date = self._expirations[date]
            options = self._download_options(date, proxy=proxy)

        return _namedtuple('Options', ['calls', 'puts'])(**{
            "calls": self._options2df(options['calls'], tz=tz),
            "puts": self._options2df(options['puts'], tz=tz)
        })

    # ------------------------

    @property
    def quote(self):
        if not self._quote:
            url = "{}/v10/finance/quoteSummary/{}?modules=price".format(self._base_url, self.ticker.replace('.', '-'))
            resp = http.get(url=url).json()
            result = resp['quoteSummary']['result'][0]
            if result['price']:
                self._quote = result['price']
            else:
                raise TickerException('Unable to fetch quote for ticker {}: {}'.format(self.ticker, resp))
        return self._quote

    @property
    def isin(self):
        return self.get_isin()

    @property
    def major_holders(self):
        return self.get_major_holders()

    @property
    def institutional_holders(self):
        return self.get_institutional_holders()

    @property
    def dividends(self):
        return self.get_dividends()

    @property
    def dividends(self):
        return self.get_dividends()

    @property
    def splits(self):
        return self.get_splits()

    @property
    def actions(self):
        return self.get_actions()

    @property
    def info(self):
        return self.get_info()

    @property
    def calendar(self):
        return self.get_calendar()

    @property
    def recommendations(self):
        return self.get_recommendations()

    @property
    def earnings(self):
        return self.get_earnings()

    @property
    def quarterly_earnings(self):
        return self.get_earnings(freq='quarterly')

    @property
    def financials(self):
        return self.get_financials()

    @property
    def quarterly_financials(self):
        return self.get_financials(freq='quarterly')

    @property
    def balance_sheet(self):
        return self.get_balancesheet()

    @property
    def quarterly_balance_sheet(self):
        return self.get_balancesheet(freq='quarterly')

    @property
    def balancesheet(self):
        return self.get_balancesheet()

    @property
    def quarterly_balancesheet(self):
        return self.get_balancesheet(freq='quarterly')

    @property
    def cashflow(self):
        return self.get_cashflow()

    @property
    def quarterly_cashflow(self):
        return self.get_cashflow(freq='quarterly')

    @property
    def sustainability(self):
        return self.get_sustainability()

    def options(self, proxy=None):
        if not self._expirations:
            self._download_options(proxy=proxy)
        return tuple(self._expirations.keys())
