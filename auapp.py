from lib2to3.pgen2.pgen import DFAState
from flask import Flask, render_template, request, jsonify, flash, redirect
import pymysql
import pandas as pd
import numpy as np
from aucalc import exceptions


supplier = Flask(__name__)

def connection():
    s = 'localhost'
    d = 'cummulative_au'
    u = 'testuser'
    p = 'charlie'
    conn = pymysql.connect(host=s, user=u, password=p, database=d)
    return conn



@supplier.route("/")
def main():
    return render_template("au.html")
    

def show_percent_column(dfCharges_row):
    if dfCharges_row['cummul_fees'] > 0:
        percent_column = round(((dfCharges_row['cummul_fees'] / dfCharges_row['cummul_throughput'])*100),2)
    else:
        percent_column = 0
    return percent_column


@supplier.route("/charges")
def charges_summary():
    charges = []
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cThru_au JOIN totalCharges_au ON cThru_au.supplier_id=totalCharges_au.supplier_id")
    # cursor.execute("SELECT * FROM totalCharges")
    for row in cursor.fetchall():
        charges.append({"supplier_id": row[0], "cummul_throughput": row[1], "supplier_id": row[2], "cummul_fees": row[3], "min_fee": row[4]})
    conn.close()
    dfCharges = pd.DataFrame(charges, columns = ['supplier_id', 'cummul_throughput', 'supplier_id', 'cummul_fees', 'min_fee'])
    dfCharges['%'] = dfCharges.apply(show_percent_column, axis=1)
    totrow = dfCharges.sum(axis = 0, skipna = True)
    print(dfCharges)
    dfCharges.to_csv('billingsummaryAU.csv', index=False)
    print(totrow)
    print(type(totrow))
    totThroughput = totrow['cummul_throughput']
    totFees = totrow['cummul_fees']
    totMinFees = totrow['min_fee']
    fees_ratio = totFees / totThroughput
    fees_percent = "{:.2%}".format(fees_ratio)
    return render_template("charges_summary.html", tables=[dfCharges.to_html(classes='data')], titles=dfCharges.columns.values, totThroughput=totThroughput, totFees=totFees, totMinFees=totMinFees, fees_percent=fees_percent)

@supplier.route("/exceptions")
def exceptions_summary():
    df = pd.DataFrame(exceptions, columns=['supplier_id','catalog_except','store_except','rate_except','rate_except_rate','manual_except','retrofit_except', 'line_cap_override'])
    df.to_csv('exceptionssummaryAU.csv', index=False)
    return render_template("exceptions_summary.html", tables=[df.to_html(classes='data')], titles=df.columns.values)

@supplier.route("/parents")
def parents():
    parentList = []
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM parentBilling_au")
    for row in cursor.fetchall():
        parentList.append({"supplier_id": row[0], "parent_id": row[1]})
    conn.close()
    return render_template("parents_summary.html", parentList=parentList)

@supplier.route("/add/parent", methods = ['GET','POST'])
def add_parent():
    if request.method == 'GET':
        return render_template("add_parent.html")
    if request.method == 'POST':
        supplier = request.form['supplierid']
        parent = request.form['parentid']
        error = None
        if not supplier or not supplier.strip():
            error = 'Supplier ID is missing'
        if not parent or not parent.strip():
            error = 'Parent ID is missing'
        if error:
            return render_template("add_parent.html", error=error, supplier=supplier, parent=parent)
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO parentBilling_au (supplier_id, parent_id) VALUES (%s, %s)", (supplier, parent))
    conn.commit()
    conn.close()
    return redirect('/')

@supplier.route("/remove/parent", methods = ['GET','POST'])
def remove_parent():
    if request.method == 'GET':
        return render_template("remove_parent.html")
    if request.method == 'POST':
        supplier = request.form['supplierid']
        parent = request.form['parentid']
        error = None
        if not supplier or not supplier.strip():
            error = 'Supplier ID is missing'
        if not parent or not parent.strip():
            error = 'Parent ID is missing'
        if error:
            return render_template("remove_parent.html", error=error, supplier=supplier, parent=parent)
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM parentBilling_au WHERE (supplier_id = %s AND parent_id = %s)", (supplier, parent))
    conn.commit()
    conn.close()
    return redirect('/')

@supplier.route("/community")
def exceptions_community():
    exceptionscommunity = []
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM commExceptions_au")
    for row in cursor.fetchall():
        exceptionscommunity.append({"supplier_id": row[0], "community_id": row[1]})
    conn.close()
    return render_template("community_summary.html", exceptionscommunity=exceptionscommunity)

@supplier.route("/add/community", methods = ['GET','POST'])
def add_community_exception():
    conn = connection()
    cursor = conn.cursor()
    if request.method == 'GET':
        cursor.execute("SELECT * FROM communities_au")
        commsTuple = cursor.fetchall()
        print(commsTuple)
        commsList = list(commsTuple)
        communities = []
        print('Communities: ', commsList)
        i = 0
        while i < len(commsList):
            singletonExtract = str(commsList[i])
            print(singletonExtract)
            newStr = singletonExtract[2:-3]
            print(newStr)
            communities.append(newStr)
            print(communities)
            i = i +1
        return render_template("add_comm_exception.html", communities=communities)
    if request.method == "POST":
        hidden_comms = request.form['hidden_comms']
        supplier = request.form['supplierid']
        man_over = request.form['manover']
        ret_over = request.form['retover']
        error = None
        if not hidden_comms or not hidden_comms.strip():
            error = 'Community exclusions are missing'
        if not supplier or not supplier.strip():
            error = 'Supplier ID is missing'
        if error:
            return render_template("add_comm_exception.html", error=error, hidden_comms=hidden_comms, supplier=supplier, man_over=man_over, ret_over=ret_over)
        print('hidden comms', hidden_comms)
        print('supplier', supplier)
        print('manover', man_over)
        print('retover', ret_over)
        hidden_comms_list = hidden_comms.split(",")
        print('hidden comms list', hidden_comms_list)
        for comm in hidden_comms_list:
            print('comm', comm)
            commStr = str(comm)
            cursor.execute("INSERT INTO commExceptions_au VALUES (%s, %s)", (supplier, commStr, man_over, ret_over))
            conn.commit()
            msg = "record added"
        return render_template('na.html', hidden_comms=hidden_comms)
    return redirect('/')                      

@supplier.route("/remove/community", methods = ['GET','POST'])
def remove_community_exception():
    if request.method == 'GET':
        return render_template("remove_comm_exception.html")
    if request.method == 'POST':
        supplier = request.form['supplierid']
        community = request.form['communityid']
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM commExceptions_au WHERE (supplier_id = %s AND community_id = %s)", (supplier, community))
    conn.commit()
    conn.close()
    return redirect('/')

@supplier.route("/orders", methods = ['GET', 'POST'])
def orders_supplier():
    if request.method == 'GET':
        return render_template("ordersbysupplier.html")
    if request.method == 'POST':
        supplier = request.form['supplierid']
    supplierorders = []
    # data = [
    #     ("Jan", 1597),
    #     ("Feb", 1501),
    #     ("Mar", 1320),
    #     ("Apr", 1600),
    #     ("Jun", 1209),
    #     ("Jul", 1401)
    # ]
    labels = [
      'Jan',
      'Feb',
      'Mar',
      'Apr',
      'May',
      'Jun',
    ]
    values = [0, 10, 5, 2, 20, 30, 45]
    conn = connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders_au WHERE supplier_id = %s", supplier)
    for row in cursor.fetchall():
        supplierorders.append({"order_id": row[0], "community_id": row[1], "supplier_id": row[2], "supplier_type": row[3], "product code": row[4], "product_type": row[5], "localised_throughput": row[6]})
    conn.close()
    dfsuppOrders = pd.DataFrame(supplierorders, columns=['order_id', 'community_id', 'supplier_id', 'supplier_type', 'product_code', 'product_type', 'localised_throughput'])
    dfsuppOrders.to_csv('ordersbysuppAU.csv', index=False)
    # labels = []
    # values = []
    # for row in data:
    #     labels.append(row[0])
    #     values.append(row[1])
    return render_template("supplier_orders.html", labels=labels, values=values, tables=[dfsuppOrders.to_html(classes='data')], titles=dfsuppOrders.columns.values)

if(__name__ == "__main__"):
    supplier.run(debug=False, host = "127.0.0.1", port=4500)
