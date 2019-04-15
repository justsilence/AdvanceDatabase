from bson import ObjectId
from flask import Flask, render_template, request, redirect, url_for, session
import numpy
import pickle
import pandas as pd
from collections import defaultdict

from neo4j import GraphDatabase
from pymongo import MongoClient

app = Flask(__name__)

app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

client = MongoClient('localhost', 27017)
mongo_db = client.bank_info

confi_coll = mongo_db.confidential
user_coll = mongo_db.customer_tb
teller_coll = mongo_db.teller
cust_acct = mongo_db.cust_acct
trans_coll = mongo_db.transactions



driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "lv23623600"))


@app.route('/', methods=['GET', 'POST'])
def home():
    return 'Hello World!'


# 88528785@test.com
# loj600073
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')

    elif request.method == 'POST':
        id = str(request.form.get('inputEmail'))
        special_str = "@"
        pos = int(id.find(special_str)) + 3
        id = id[:-pos]
        passwd = request.form.get('inputPassword')
        user = confi_coll.find_one({'ID': id, 'passwd': str(passwd)})
        print(user)
        if user is not None:
            session['user_id'] = user['ID']
        session['logged_in'] = True
        return redirect(url_for('home'))


@app.route('/profile', methods=['GET', 'POST'])
def profile():
    user_id = session.get('user_id')
    print(user_id)

    check_user_type = str(user_id)

    if len(check_user_type) == 8:
        user_transaction_table = get_user_transaction(user_id)
        session['user_table'] = user_transaction_table
        return render_template("profile.html", cards= user_transaction_table.keys())
    elif len(check_user_type) == 6:
# IB0009@test.com
# dip164087
        teller_transaction = get_teller_transaction(user_id)
        session['teller_transaction'] = teller_transaction
        return render_template('teller_profile.html')
    return redirect(url_for('home'))

@app.route('/teller_profile', methods=['GET', 'POST'])
def teller_profile():
    # teller_transaction = session.get('teller_transaction')
    user_id = session.get('user_id')
    teller_transaction = get_teller_transaction(user_id)
    session['teller_transaction'] = teller_transaction
    daily_amount,time_interval_transaction,time_interval_amount,card_transaction, major = analysis_teller(teller_transaction, time1 = "2018-08-16", time2 = "2018-08-18")  
    

    daily_amount = [[ str(item) for item in line]  for line in daily_amount.reset_index().values.tolist()]
    if time_interval_amount:
        time_interval_transaction = time_interval_transaction.reset_index().values

    card_transaction = card_transaction.reset_index().values


    return render_template('teller_profile.html', daily_amount=daily_amount, time_interval_transaction=time_interval_transaction, time_interval_amount=time_interval_amount, card_transaction=card_transaction, major=major)
    # return render_template('teller_profile.html', major=major)


@app.route('/account_detail/<int:id>', methods=['GET', 'POST'])
def account_detail(id):
    user_transaction_table = session.get('user_table')

    if request.method == 'GET':
        info = session.get('info')
        flag = True
        if info == None:
            info = []
            flag = False
        return render_template('account_detail.html', id=int(id), account_table=user_transaction_table[str(id)], flag=flag, info=info)

    elif request.method == 'POST':
        start_date = request.form.get('start', None)
        end_date = request.form.get('end', None)

        if not start_date or not end_date:
            start_date = None
            end_date = None


        # user_table = session.get('user_table')

        card_transaction = get_card_transaction(str(id),user_transaction_table)
        
        # [sum_in, sum_out, time_sum_out, time_sum_in, difference, time_difference] = list(analysis_account(card_transaction, start_date, end_date))
        info = list(map(str,list(analysis_account(card_transaction, start_date, end_date))))
        session['info'] = info
        return redirect(url_for('account_detail', id=id))


def isLoggedIn():
    logged_in = False
    if session is not None:
        if session['logged_in'] is not None:
            logged_in = session['logged_in']
    print(logged_in)
    session['logged_in'] = logged_in
    return logged_in




def get_card_transaction(card_id,user_transaction_table):
    all_trans_for_an_card = user_transaction_table[card_id]
    card_transaction = pd.DataFrame(data = all_trans_for_an_card, index = range(len(all_trans_for_an_card)), columns = ["MY CARD","TO CARD","TRAN TIME","TRAN NUMBER", "TRAN AMOUNT","OPR ID","FLAG" ] )
    card_transaction["TRAN TIME"] = pd.to_datetime(card_transaction["TRAN TIME"])
    card_transaction = card_transaction.set_index("TRAN TIME").sort_index()
    return card_transaction


def analysis_account(card_transaction, time1 = None, time2 = None):
    
    sum_in = None
    sum_out = None
    time_sum_in = None
    time_sum_out = None
    difference = None
    time_difference = None
    
    groups = card_transaction.groupby(["FLAG"])
    for name, group in groups:
        if name == -1:
            sum_out = group["TRAN AMOUNT"].sum()
        else:
            sum_in = group["TRAN AMOUNT"].sum()
        
        if time1 and time2:
            group = group[time1:time2]
            if name == -1:
                time_sum_out = group["TRAN AMOUNT"].sum()
            else:
                time_sum_in = group["TRAN AMOUNT"].sum()
            
            time_difference = time_sum_in - time_sum_out

    difference = sum_in - sum_out
    
    
    return sum_in, sum_out, time_sum_out, time_sum_in, difference, time_difference



def get_user_transaction(user_id):
    with driver.session() as session:
        res = session.run("match (a:Account) -[:belongTo]-> (c:Customer { id : $id }) return a", id = user_id )
        all_account = []
        for record in res:
            for item in record:
                all_account.append(item["id"])
        
        user_transaction_table = defaultdict(list)
        
        # 转出
        for account_id in all_account:
            res = session.run("match (a:Account {id : $id}) -[r:transaction]-> (b:Account) return r,a,b",id = account_id)
            for record in res:
                user_transaction_table[account_id].append([record['a']['id'],record['b']['id'],record['r']['TR_TM'],record['r']['TR_NO'],record['r']['TR_AM'],record['r']['OPR_id'],-1])
            
        # 转入
        for account_id in all_account:
            res = session.run("match (a:Account {id : $id}) <-[r:transaction]- (b:Account) return r,a,b",id = account_id)
            for record in res:
                user_transaction_table[account_id].append([record['a']['id'],record['b']['id'],record['r']['TR_TM'],record['r']['TR_NO'],record['r']['TR_AM'],record['r']['OPR_id'],1])
        
        return user_transaction_table



def get_teller_transaction(teller_id):
    with driver.session() as session:
        res = session.run("match (t:Teller { id : $id }) -[:serve]-> (a:Account) return a", id = teller_id )
        all_account = []
        for record in res:
            for item in record:
                all_account.append(item["id"])
        
        teller_transaction = []
        
        # 转出
        for account_id in all_account:
            res = session.run("match (a:Account {id : $acc_id}) -[r:transaction]-> (b:Account) where r.OPR_id = $opr_id return r,a,b",acc_id = account_id, opr_id = teller_id )
            for record in res:
                teller_transaction.append([record['a']['id'],record['b']['id'],record['r']['TR_TM'],record['r']['TR_NO'],record['r']['TR_AM'],record['r']['OPR_id']])
        
        return teller_transaction


def analysis_teller(teller_transaction, time1 = None, time2 = None):
    teller_transaction = pd.DataFrame(data = teller_transaction, index = range(len(teller_transaction)), columns = ["FROM CARD","TO CARD","TRAN TIME","TRAN NUMBER", "TRAN AMOUNT","OPR ID"] )
    teller_transaction["TRAN TIME"] = pd.to_datetime(teller_transaction["TRAN TIME"])
    teller_transaction = teller_transaction.set_index("TRAN TIME").sort_index()

    daily_amount = None
    time_interval_transaction = None
    time_interval_amount = None
    card_transaction = None
    major_account = None
    max_value = None
    
    if time1 and time2:    
        time_interval_transaction = teller_transaction[time1:time2]
        time_interval_amount = time_interval_transaction["TRAN AMOUNT"].sum()

    daily_amount = teller_transaction.groupby("TRAN TIME")["TRAN AMOUNT"].sum()
    card_transaction = teller_transaction.groupby("FROM CARD")["TRAN AMOUNT"].sum()
    major_account,max_value = card_transaction.idxmax(),card_transaction.max()


    return daily_amount,time_interval_transaction,time_interval_amount,card_transaction,[str(major_account),str(max_value)]



if __name__ == '__main__':
    app.run()
