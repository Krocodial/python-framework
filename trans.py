import falcon, json, redis, jinja2, os, threading, datetime, time

import locale, socket, gevent

from socketpool import ConnectionPool, TcpConnector

from xml.etree.ElementTree import Element, SubElement, tostring
#from defusedxml.ElementTree as ET
import defusedxml
from ast import literal_eval
import app.db as db
from app.helper import clean_up, get_quote, split_money, get_cents, stock_buy_trigger, stock_sell_trigger
import logging
#experimental cython object compilation on import
#import pyximport; pyximport.install(pyimport=True)
from app.log import LogMiddleware

MODE = os.getenv('MODE', 'DEV')
db.initialize_db()
defusedxml.defuse_stdlib()

#https://redis-py.readthedocs.io/en/latest/
redis_master_pool = redis.ConnectionPool(host=os.getenv('REDIS_MASTER_HOST', '127.0.0.1'), port=6379, db=0, decode_responses=True)
redis_slave_pool = redis.ConnectionPool(host=os.getenv('REDIS_SLAVE_HOST', '127.0.0.1'), port=os.getenv('REDIS_SLAVE_PORT', 6379), db=0, decode_responses=True)
redis_num_pool = redis.ConnectionPool(host=os.getenv('REDIS_MASTER_HOST', '127.0.0.1'), port=6379, db=1, decode_responses=True)

if MODE == 'DEV':
  sock_addr = 'localhost'
  log_file = 'logs/falcon.log'
elif MODE == 'DEV-DOCKER':
  sock_addr = 'quote'
  log_file = 'logs/falcon.log'#'/usr/src/app/logs/falcon.log'
else:
  sock_addr = '192.168.1.100'
  log_file = 'logs/falcon.log'#'/usr/src/app/logs/falcon.log'

logging.basicConfig(format='%(message)s', filename=log_file, filemode='a+')
logger = logging.getLogger('falcon_logger')

options = {'host': sock_addr, 'port': 4441}
pool = ConnectionPool(factory=TcpConnector, backend="gevent")

#with redis.Redis(connection_pool=redis_num_pool) as conn:
#    conn.set('transNum', 1)

#Import jinja template rendering and enable autoescapes (prevents xss)
from jinja2 import Environment, PackageLoader, select_autoescape
env = Environment(
    loader=PackageLoader('app.trans', 'templates'),
    autoescape=select_autoescape(default=True)
)


class IndexResource(object):
    command = 'INDEX'
    def on_get(self, req, resp):
        #req.context['command'] = 'INDEX'
        template = env.get_template('index.html')
        resp.content_type = 'text/html'
        resp.body = template.render()


class HomeResource(object):
    def on_get(self, req, resp):
        """Handles GET requests"""
        resp.status = falcon.HTTP_200  # This is the default status
        resp.body = ('\nTwo things awe me most, the starry sky '
                     'above me and the moral law within me.\n'
                     '\n'
                     '    ~ Immanuel Kant\n\n')


class UserAccoutResource(object):
    command = 'DISPLAY_SUMMARY'
    def on_get(self, req, resp):
        #print("get acc")
        username = req.params['username']
        account = db.get_account(username)
        if (account):
            resp.body = (json.dumps(account))
        else:
            resp.status = falcon.HTTP_501
            resp.body = (' failed ')


class UserAddBalanceResource(object):
    command = 'ADD'
    def log_account_transaction(req, resp, resource):
        if resp.status == falcon.HTTP_200:
            timestamp = int(time.time() * 1000)
            server = 'transaction'
            transactionNum = req.context.transactionNum
            log = {
                'timestamp': timestamp, 
                'server': server, 
                'transactionNum': transactionNum, 
                'action': 'add',
                'username': req.get_param('username'),
                'funds': req.get_param('balance')}
            log['type'] = 'accountTransaction'
        
            logger.info(json.dumps(log)) 
            #with redis.Redis(connection_pool=redis_master_pool) as conn:
            #    conn.lpush('log', str(log))


    @falcon.after(log_account_transaction)
    def on_post(self, req, resp):
        #print("add balnce")
        
        username = req.get_param('username')
        amount = split_money(req.get_param('balance'))
        if(db.create_user(username, amount)):
            resp.status = falcon.HTTP_201
        else:
            if(db.user_add_balance(username, amount)):
                resp.status = falcon.HTTP_200
            else:
                resp.status = falcon.HTTP_501


#Request input {'id': username, 'stocksymbol': stock}
class QuoteGetResource(object):
    command = 'QUOTE'
    #log what we are returning to the user
    def log_quote(req, resp, resource):
        if 'cryptokey' in literal_eval(resp.body):
          timestamp = int(time.time() * 1000)
          server = 'transaction'
          transactionNum = req.context.transactionNum
          log = {'timestamp': timestamp, 'server': server, 'transactionNum': transactionNum}
          log.update(literal_eval(resp.body))
          log['type'] = 'quoteServer'
          log['command'] = 'quote'

          log['price'] =  str(log['amount'])
          #del log['dollar']
          #del log['cent']
          logger.warning(json.dumps(dict((k, log[k]) for k in ('timestamp', 'server', 'transactionNum', 'price', 'stockSymbol', 'username', 'quoteServerTime', 'cryptokey', 'type'))))
          #with redis.Redis(connection_pool=redis_master_pool) as conn:
          #  conn.lpush('log', str(dict((k, log[k]) for k in ('timestamp', 'server', 'transactionNum', 'price', 'stockSymbol', 'username', 'quoteServerTime', 'cryptokey', 'type'))))

    @falcon.after(log_quote)
    def on_get(self, req, resp):
        #print("quote")
        username = req.params['username']
        stock = req.params['stock']

        data = {'username': username, 'stockSymbol': stock}
        quote = get_quote(stock, username, req)

        #template = env.get_template('stock.j2')
        resp.status = falcon.HTTP_200
        quote.update(data)
        resp.body = json.dumps(quote)
        #resp.content_type = 'text/html'
        #resp.body = template.render(dollar=quote['dollar'], cent=quote['cent'], stock=stock, user=username)

#If the user has enough cash, place a hold on the amount and add buy order to redis 'username' queue
class StockBuyResource(object):
    command = 'BUY'
    def on_post(self, req, resp):
        username = req.get_param('username')
        stock = req.get_param('stock')
        amount = req.get_param('amount')

        quote = get_quote(stock, username, req)

        given_cents = split_money(amount)
        stock_cents = int(quote['amount'])

        purchasable = given_cents // stock_cents

        cost = stock_cents*purchasable

        #Assume the user exists, this should ideally check..
        user_total = db.get_user_balance(username)

        if user_total >= cost and purchasable > 0:
            resp.status = falcon.HTTP_200
            req.context.master_conn.lpush(username+"_buy", str(datetime.datetime.now().timestamp()) + ':' + str(cost) + ':' + stock + ':' + str(purchasable))

            #with redis.Redis(connection_pool=redis_master_pool) as conn:
            #    conn.lpush(username+"_buy", str(datetime.datetime.now().timestamp()) + ':' + str(cost) + ':' + stock + ':' + str(purchasable))
            t = threading.Thread(target=clean_up, args=(username, "_buy"))
            t.start()
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_402

        #template = env.get_template('index.html')
        #resp.content_type = 'text/html'
        #resp.body = template.render()


class StockBuyCommitResource(object):
    command = 'COMMIT_BUY'
    def on_post(self, req, resp):
        #print("stock buy commit")
        username = req.get_param('username')
        val = req.context.master_conn.lpop(username+"_buy")
        #with redis.Redis(connection_pool=redis_master_pool, decode_responses=True) as conn:
        #    val = conn.lpop(username+"_buy")

        if val is None:
            resp.status = falcon.HTTP_402
        else: 
            ts,amount,stock,num = val.split(':')

            if(db.buy_stock(username, stock, int(num), int(amount))):
                resp.status = falcon.HTTP_200
                #resp.body = (' success: {} {} {} \n'.format(id, code, amount))
            else:
                resp.status = falcon.HTTP_501
                #resp.body = (' internal server error ')


class StockBuyCancelResource(object):
    command = 'CANCEL_BUY'
    def on_post(self, req, resp):
        req.context['command'] = 'CANCEL_BUY'
        #print("stock buy cancel")
        username = req.get_param('username')
        req.context.master_conn.lpop(username+"_buy")
        #with redis.Redis(connection_pool=redis_master_pool, decode_responses=True) as conn:
        #    val = conn.lpop(username+"_buy")
        resp.status = falcon.HTTP_200


class StockBuySetResource(object):
    command = 'SET_BUY_AMOUNT'
    def on_post(self, req, resp):
        #print("stock set buy")

        username = req.get_param('username')
        stock_code = req.get_param('stock')
        stock_amount = split_money(req.get_param('amount'))

        if (db.hold_user_funds(username, stock_code, stock_amount)):
            req.context.master_conn.lpush(username+"_buy_"+stock_code , str(datetime.datetime.now().timestamp()) + ":" + str(stock_code) + ":" + str(stock_amount))

            #with redis.Redis(connection_pool=redis_master_pool) as conn:
            #    conn.lpush(username+"_buy_"+stock_code , str(datetime.datetime.now().timestamp()) + ":" + str(stock_code) + ":" + str(stock_amount))
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_410


class StockBuySetTriggerResource(object):
    command = 'SET_BUY_TRIGGER'
    def on_post(self, req, resp):
        #print("stock buy set trigger")
        username = req.get_param('username')
        stock_code = req.get_param('stock')
        stock_price = split_money(req.get_param('price'))

        val = req.context.master_conn.lpop(username+'_buy_'+stock_code)
        #with redis.Redis(connection_pool=redis_master_pool, decode_responses=True) as conn:
        #    val = conn.lpop(username+'_buy_'+stock_code)

        if val is None:
            resp.status = falcon.HTTP_400
        else:

            ts,stock,amount = val.split(':')

            t = threading.Thread(target=stock_buy_trigger, args=(username, stock_code, int(stock_price), int(amount), req))
            t.start()

            resp.status = falcon.HTTP_200


class StockBuySetCancelResource(object):
    command = 'CANCEL_SET_BUY'
    def on_post(self, req, resp):
        #print("stock buy set cancel")
        username = req.get_param('username')
        stock_code = req.get_param('stock')

        req.context.master_conn.delete(username + '_buy_' + stock_code)
        #with redis.Redis(connection_pool=redis_master_pool) as conn:
        #    conn.delete(username + '_buy_' + stock_code)

        db.release_user_funds(username, stock_code)
        resp.status = falcon.HTTP_200


class StockSellResource(object):
    command = 'SELL'
    def log_account_transaction(req, resp, resource):
        if resp.status == falcon.HTTP_200:
            timestamp = int(time.time() * 1000)
            server = 'transaction'
            transactionNum = req.context.transactionNum
            log = {
                'timestamp': timestamp, 
                'server': server, 
                'transactionNum': transactionNum, 
                'action': 'sell',
                'username': req.get_param('username'),
                'funds': req.get_param('amount')}
            log['type'] = 'accountTransaction'
            logger.warning(json.dumps(log))    
            #with redis.Redis(connection_pool=redis_master_pool) as conn:
            #    conn.lpush('log', str(log))


    @falcon.after(log_account_transaction)
    def on_post(self, req, resp):
        #print("stock sell ")
        username = req.get_param('username')
        stock_code = req.get_param('stock')
        price_amount = req.get_param('amount')

        quote = get_quote(stock_code, username, req)
        given_cents = split_money(price_amount)

        #how many stocks purchasable with price_amount
        stock_cents = int(quote['amount'])
        purchasable = given_cents // stock_cents

        gain = stock_cents * purchasable

        if( purchasable > 0 and db.user_has_stock(username, stock_code, purchasable)):
            req.context.master_conn.lpush(username+"_sell", str(datetime.datetime.now().timestamp()) + ':' + str(gain) + ':' + stock_code + ':' + str(purchasable))
            #with redis.Redis(connection_pool=redis_master_pool) as conn:
            #    conn.lpush(username+"_sell", str(datetime.datetime.now().timestamp()) + ':' + str(gain) + ':' + stock_code + ':' + str(purchasable))
            t = threading.Thread(target=clean_up, args=(username, "_sell"))
            t.start()
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_501


class StockSellCommitResource(object):
    command = 'COMMIT_SELL'
    def on_post(self, req, resp):
        #print("stock sell commit")
        username = req.get_param('username')
        val = req.context.master_conn.lpop(username+"_sell")
        #with redis.Redis(connection_pool=redis_master_pool, decode_responses=True) as conn:
        #    val = conn.lpop(username+"_sell")

        if val is None:
            resp.status = falcon.HTTP_410
        else:
            ts,amount,stock,num = val.split(':')

            if(db.sell_stock(username, stock, int(num), int(amount))):
                resp.status = falcon.HTTP_200
            else:
                resp.status = falcon.HTTP_501


class StockSellCancelResource(object):
    command = 'CANCEL_SELL'
    def on_post(self, req, resp):
        #print("stock sell cancel")
        username = req.get_param('username')
        val = req.context.master_conn.lpop(username+"_sell")
        #with redis.Redis(connection_pool=redis_master_pool, decode_responses=True) as conn:
        #    val = conn.lpop(username+"_sell")
        if val is None:
            resp.status = falcon.HTTP_410
        else:
            resp.status = falcon.HTTP_200


class StockSellSetResource(object):
    command = 'SET_SELL_AMOUNT'
    def on_post(self, req, resp):
        #print("stock sell set")
        username = req.get_param('username')
        stock_code = req.get_param('stock')
        stock_amount = split_money(req.get_param('amount'))

        req.context.master_conn.lpush(username+"_sell_"+stock_code, str(datetime.datetime.now().timestamp()) + ":" + str(stock_code) + ":" + str(stock_amount))
        #with redis.Redis(connection_pool=redis_master_pool) as conn:
        #    conn.lpush(username+"_sell_"+stock_code, str(datetime.datetime.now().timestamp()) + ":" + str(stock_code) + ":" + str(stock_amount))
        resp.status = falcon.HTTP_200


class StockSellSetTriggerResource(object):
    command = 'SET_SELL_TRIGGER'
    def on_post(self, req, resp):
        #print("stock sell trigger")
        username = req.get_param('username')
        stock_code = req.get_param('stock')
        stock_price = split_money(req.get_param('price'))

        r_val = req.context.master_conn.lpop(username+"_sell_"+stock_code)
        #with redis.Redis(connection_pool=redis_master_pool, decode_responses=True) as conn:
        #    r_val = conn.lpop(username+"_sell_"+stock_code)
        if r_val is not None:
            ts, code, amount = r_val.split(":")
            if db.create_reserve_stock(username, stock_code, stock_price, int(amount)):
                t = threading.Thread(target=stock_sell_trigger, args=(username, stock_code, stock_price, int(amount), req))
                t.start()
                resp.status = falcon.HTTP_200
            else:
                resp.status = falcon.HTTP_410
        else:
            resp.status = falcon.HTTP_410



class StockSellSetCancelResource(object):
    command = 'CANCEL_SET_SELL'
    def on_post(self, req, resp):
        username = req.get_param('username')
        stock_code = req.get_param('stock')
        
        req.context.master_conn.delete(username + "_sell" + stock_code)
        #with redis.Redis(connection_pool=redis_master_pool) as conn:
        #    conn.delete(username + "_sell" + stock_code)

        db.cancel_set_sell(username, stock_code)
        resp.status = falcon.HTTP_200


class StaticResource(object):
    def on_get(self, req, resp, filename):
        resp.status = falcon.HTTP_200
        if filename.endswith('css'):
            with open('app/proxy/static/css/' + filename, 'r') as f:
                resp.body = f.read()
        elif filename.endswith('js'):
            with open('app/proxy/static/js/' + filename, 'r') as f:
                resp.body = f.read()


class LogResource(object):
    command = 'DUMPLOG'
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        data = Element('log')
        with open(log_file, 'r') as f:
          for line in f:
              dic = json.loads(line)
              typ = SubElement(data, dic['type'])
              del dic['type']
              for item,value in dic.items():
                  tmp = SubElement(typ, item)
                  tmp.text = str(value)#.strip("\n")
        resp.body = tostring(data)
        filename = req.params.get('filename', 'logs.xml')
        resp.downloadable_as = filename


# falcon.API instances are callable WSGI apps
app = falcon.API(middleware=[LogMiddleware(redis_master_pool, redis_slave_pool, redis_num_pool, logger)])
app.req_options.auto_parse_form_urlencoded = True #append POSTed form parameters to req.params
app.resp_options.secure_cookies_by_default = False

# Resources are represented by long-lived class instances
index = IndexResource()
home = HomeResource()

#user
user_add_balance = UserAddBalanceResource()
user_account = UserAccoutResource()

#stock
stock_buy = StockBuyResource()
stock_buy_commit = StockBuyCommitResource()
stock_buy_cancel = StockBuyCancelResource()

stock_buy_set = StockBuySetResource()
stock_buy_set_trigger = StockBuySetTriggerResource()
stock_buy_set_cancel = StockBuySetCancelResource()

stock_sell = StockSellResource()
stock_sell_commit = StockSellCommitResource()
stock_sell_cancel = StockSellCancelResource()

stock_sell_set = StockSellSetResource()
stock_sell_set_trigger = StockSellSetTriggerResource()
stock_sell_set_cancel = StockSellSetCancelResource()

quote_get = QuoteGetResource()

logs = LogResource()

app.add_route('/', index)
app.add_route('/home', home)

app.add_route('/api/user/add', user_add_balance)            #ADD
app.add_route('/api/user/account', user_account)            #DISPLAY_SUMMARY

app.add_route('/api/stock/buy', stock_buy)                  #BUY
app.add_route('/api/stock/buy/commit', stock_buy_commit)    #COMMIT_BUY
app.add_route('/api/stock/buy/cancel', stock_buy_cancel)    #CANCEL_BUY

app.add_route('/api/stock/buy/set', stock_buy_set)                  #SET_BUY_AMOUNT
app.add_route('/api/stock/buy/set/trigger', stock_buy_set_trigger)  #SET_BUY_TRIGGER
app.add_route('/api/stock/buy/set/cancel', stock_buy_set_cancel)    #CANCEL_SET_BUY

app.add_route('/api/stock/sell', stock_sell)                #SELL
app.add_route('/api/stock/sell/commit', stock_sell_commit)  #COMMIT_SELL
app.add_route('/api/stock/sell/cancel', stock_sell_cancel)  #CANCEL_SELL

app.add_route('/api/stock/sell/set', stock_sell_set)                 #SET_SELL_AMOUNT
app.add_route('/api/stock/sell/set/trigger', stock_sell_set_trigger) #SET_SELL_TRIGGER
app.add_route('/api/stock/sell/set/cancel', stock_sell_set_cancel)   #CANCEL_SET_SELL

app.add_route('/api/quote',quote_get )                      #QUOTE

app.add_route('/api/logs', logs)                        #DUMPLOG

if MODE=='DEV':
    app.add_route('/static/css/{filename}', StaticResource())
    app.add_route('/static/js/{filename}', StaticResource())
    #app.add_static_route('/static', os.path.abspath('app/static/'), fallback_filename='css/stock.css')

