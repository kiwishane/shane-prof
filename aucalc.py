from modulefinder import AddPackagePath
import pymysql.cursors
import pandas as pd
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class ContextFilter(logging.Filter):
    supp = ['1626']

    def filter(self,record):
        record.sup = ContextFilter.supp
        return True

# logging.basicConfig(filename='AUtransactions.log', level=logging.INFO)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('AUtransactions.log')
file_handler.addFilter(lambda record: ContextFilter)
logger.addHandler(file_handler)

connection =  pymysql.connect(host = 'localhost',
            user = 'testuser',
            password = 'charlie',
            database = 'cummulative_au',
            cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()

class my_dictionary(dict):
    def __init__(self):
        self = dict()
        

    def add(self, key, value):
        self[key] = value

new_dict = []
parent_dict = []
thru = []

rate = 0.0125
linecapValue = 10000    #change to 9999999999 for NA
charges = my_dictionary()
cummul = my_dictionary()
newPar = my_dictionary()
tempDict = my_dictionary()
chargesList = []
parents = []


# cursor.execute("SELECT * from cThru")
# thru = cursor.fetchall()
# print(thru)
# if isinstance(thru, list):
#     print('thru is a list')
# else:
#     print('thru is not a list')
#     thru = []


cursor.execute("SELECT * FROM rateExceptions_au")
exceptions = cursor.fetchall()
print(exceptions)

cursor.execute("SELECT * FROM commExceptions_au")
comm_exceptions = cursor.fetchall()
print(comm_exceptions)

df = pd.DataFrame(comm_exceptions, columns=['supplier_id','community_id'])
print(df)

cursor.execute("SELECT * FROM parentBilling_au")
parents = cursor.fetchall()
print('Parents are:    ', parents)

dfParents = pd.DataFrame(parents, columns=['supplier_id','parent_id'])
print(dfParents)

def replace_values(list_to_replace, item_to_replace, item_to_replace_with):
    return[item_to_replace_with if item == item_to_replace else item for item in list_to_replace]


def selectOrderLine(row):
    global active_line
    global active_supplier
    global active_supplier_type
    global active_throughput
    global active_community
    global active_order
    global active_product_code
    global active_product
    active_line = row
    active_supplier = row['supplier_id']
    active_supplier_type = row['supplier_type']
    active_throughput = row['localised_throughput']
    active_community = row['community_id']
    active_order = row['order_id']
    active_product_code = row['product_code']
    logger.info(f"ACTIVE PRODUCT CODE from select order line = {active_product_code}")
    active_product = row['product_type']
    logger.info(f"ACTIVE PRODUCT from select order line = {active_product}")
    return[active_supplier, active_supplier_type, active_throughput, active_community, active_order, active_product_code, active_product, active_line]

def checkCommunityExceptions(supp,comm):
    global commEx
    global existing
    global manualOverride
    global retroOverride
    global updateRequired
    commEx = 'n'
    manualOverride = 'n'
    retroOverride = 'n'
    updateRequired = 'y'
    existing = next((sub for sub in thru if sub['supplier_id'] == supp), None)
    # print(existing)
    for dir in comm_exceptions:
        # print(dir)
        # determine if a community exception applies for supplier and if it does not apply for manual or retro orders
        if ((dir['supplier_id'] == supp) and (dir['community_id'] == comm)):
            commEx = 'y'
            updateRequired = 'n'
            if (dir['comm_man_override'] == 'y'):
                manualOverride = 'y'
                commEx = 'n'
                updateRequired = 'y'
            else:
                if (dir['comm_retro_override'] == 'y'):
                    retroOverride = 'y'
                    commEx = 'n'
                    updateRequired = 'y'
        # else:
        #     commEx = 'n'
    return[commEx,existing,manualOverride,retroOverride,updateRequired]


def checkManualRetroExceptions(su, active_product, manualOverride, retroOverride, updateRequired, active_product_code):
    global order_excepts
    global manualEx
    global retroEx
    global zerorate_flag
    manualEx = 'n'
    retroEx = 'n'
    quoteEx = 'n'
    productEx = 'n'
    manu = 'n'
    retro = 'n'
    quote = 'n'
    zerorate_flag = 'n'
    if updateRequired == 'n':
        zerorate_flag = 'y'
    #if there are no specific overrides check if the supplier has general manual or retrofit exceptions which apply
    order_excepts = next((sub for sub in exceptions if sub['supplier_id'] == su), None)
    if order_excepts:
        logger.info(f"ACTIVE PRODUCT CODE from manualretroexcepts = {active_product_code}")
        logger.info(f"ACTIVE PRODUCT from manualretroexcept = {active_product}")
        # logger.info('Order Excepts: ', order_excepts)
        retro = order_excepts['retrofit_except']
        manu = order_excepts['manual_except']
        quote = order_excepts['quote_except']
        product = order_excepts['product_except']
        producttext = order_excepts['product_except_text']
        # logger.info('product text = ', producttext)
        if manu == 'y' and active_product == "MANUAL":
            zerorate_flag = 'y'
            updateRequired = 'n'
            if manualOverride == 'y':   #manual override means the supplier can charge for the order and therefore manualEx is set to 'n'     
                zerorate_flag = 'n'
                updateRequired = 'y'          
        elif retro == 'y' and active_product == "RETROFIT_RELEASE_ORDER":
            zerorate_flag = 'y'
            updateRequired = 'n'
            if retroOverride == 'y':  #retroOverride means the supplier can charge for the retrofit order and therefore retroEx is 'n'
                zerorate_flag = 'n'
                updateRequired = 'y'
        elif quote == 'y' and active_product == "QUOTE":
            zerorate_flag = 'y'
            updateRequired = 'n'
        else:
            if producttext is not None:
                productStr = str(product)
                producttextStr = str(producttext)
                active_product_codeStr = str(active_product_code)
                logger.info("product flag y or n %s" %(productStr))
                logger.info("product code: %s" %(active_product_codeStr))
                logger.info("product text: %s" %(producttextStr))
                if producttextStr in active_product_codeStr:
                    logger.info('RESI MATCH!')
                    zerorate_flag = 'y'
                    updateRequired = 'n'
        print(active_product)
        print(zerorate_flag)
        logger.info(f"UPDATE REQUIRED? = {updateRequired}")
    return [manualEx, retroEx, updateRequired, zerorate_flag]


def checkExceptions(su, orderThru, comm, active_product_code, product):
    global order_excepts
    global sp_rate
    global special_rate_flag
    global zerorate_flag
    global updateDecision
    global cap_override
    global df
    special_rate_flag = 'n'
    cap_override = 'n'
    sp_rate = 0.0125
    checkCommunityExceptions(su,comm)
    checkManualRetroExceptions(su,product, manualOverride,retroOverride, updateRequired, active_product_code)
    updateDecision = updateRequired
    logger.info(f"UPDATE DECISION? = {updateDecision}")
    order_excepts = next((sub for sub in exceptions if sub['supplier_id'] == su), None)
    print('Exceptions are: ', order_excepts)
    if order_excepts:    #just means exceptions have been found for the supplier
        if order_excepts['catalog_except'] == 'y':
            # print('+++++++++++++catalog exception for this supplier')
            zerorate_flag = 'y'
        elif order_excepts['store_except'] == 'y':
            # print('OOOOOOOOOO: this supplier is an internal store')
            zerorate_flag = 'y'
        elif order_excepts['rate_except'] == 'y':
            # print('PPPPPPPPPPPPP: Exclusive rate for this supplier is: ', order_excepts['rate_except_rate'])
            sp_rate = order_excepts['rate_except_rate']
            special_rate_flag = 'y'
            if order_excepts['line_cap_override'] == 'y':
                cap_override = 'y'
        elif order_excepts['line_cap_override'] == 'y':
            cap_override = 'y'
            zerorate_flag = 'n'
        else:
            print('nothinggggggggggggggg')
    else:
        print('no rate exceptions')
    return [sp_rate, zerorate_flag, special_rate_flag, cap_override, updateDecision]


def calcRate(s, thr, sp_rate, specialRF, nd, zerorate_flag):
    global line_rate
    global parents
    global parent_dict
    logger.info(f"Order line throughput: {thr}")
    parentExists = 'n' 
    getParent = next((sub for sub in parents if sub['supplier_id'] == s), None)
    if getParent:
        parentforSupplier = getParent['parent_id']
        parentExists = 'y'
        getParentThru = next((sub for sub in parent_dict if sub['parent_id'] == parentforSupplier), None)
        if getParentThru:
            updT = getParentThru['cummul_throughput']
            # logger.info(f"Revised total throughput from parent: {updT}")
    else:
        getCummulThru = next((subDict for subDict in nd if subDict['supplier_id'] == s), None)
        if getCummulThru:
            updT = getCummulThru['cummul_throughput']
            logger.info(f"CummulThru= {updT}")
        else:
            updT = thr
            logger.info(f"elseCummulThru= {updT}")
        # logger.info(f"Revised total throughput for supplier: {updT}")
    if zerorate_flag == 'y':    #checks if zero rate applies
        line_rate = 0.0000
    elif specialRF == 'y':   #checks if a special rate exists 
        print('SPECIAL SPECIAL SPECIAL')
        # updT = thr
        if updT <= 2000 and parentExists == 'n':
            line_rate = 0.0000
        else:
            line_rate = sp_rate
    elif updT > 1000000:
        line_rate = 0.0075
    elif updT > 100000:
        line_rate = 0.0100
    elif updT > 2000 and updT < 100000:
        line_rate = 0.0125
    else:
        line_rate = 0.0000
    return [line_rate]


def calcCharge(acs, at, rat):
    if cap_override == 'y':
        charge = at * Decimal(rat)
    elif at >= linecapValue:   
        charge = linecapValue * Decimal(rat)
    else:
        charge = at * Decimal(rat)
    return charge


def cummulativeCharge(acs, line_charge, zerorate_flag):
    global chargesList
    logger.info(f"active supplier: {acs}")
    # pull next dictionary {supplier, cummulative charge} from the charges list 
    current_cumm_charge = next((sub for sub in chargesList if sub['supplier_id'] == acs), None)
    logger.info(f"current charge dict = {current_cumm_charge}")

    # if there is already a charge recorded against the supplier add the line charge from the order to the current cummulative charge in the charges list
    if current_cumm_charge in chargesList:     
        current_charges_dict = current_cumm_charge
        existing_charges = current_cumm_charge['cummul_fees']     
        new_cummul_charge = existing_charges + line_charge
        logger.info(f"Updated cummulative charge in dict: {new_cummul_charge}")
        current_cumm_charge['cummul_fees'] = new_cummul_charge
        print('zerorate for cummulative   ', zerorate_flag)
        if zerorate_flag == 'n':    
            if new_cummul_charge > 31.25:
                current_cumm_charge['min_fee'] = 0.00
        # calls the replace values function and replaces the cummulative charge stored with the updated one
        new_charges_dict = replace_values(chargesList, current_charges_dict, current_cumm_charge )
    else:
        # if this is a new charge it creates a dictionary for the supplier and cummulative charge and appends it to the charges list
        new_cummul_chargeY = line_charge
        charges.key = 'supplier_id'
        charges.value = acs
        logger.info("acs: %s" %(acs))
        charges.add(charges.key, charges.value)
        logger.info(f"Add supplier to dict: {charges.key}, {charges.value}")
        charges.key1 = 'cummul_fees'
        charges.value1 = new_cummul_chargeY
        logger.info(f"new_cummul_charge: {new_cummul_chargeY}")
        charges.add(charges.key1, charges.value1)
        logger.info(f"Add throughput to dict: {charges.key1}, {charges.value1}")
        print('zerorate for cummulative   ', zerorate_flag)
        charges.key2 = 'min_fee'
        if zerorate_flag == 'n':
            if new_cummul_chargeY <= 31.25:     #made 31.25 because agreements say up to 2,500 throughput
                charges.value2 = 29.95
            else:
                charges.value2 = 0
        charges.add(charges.key2, charges.value2)
        logger.info(f"new dict added: {charges}")
        chargesList.append(charges.copy())
    return chargesList

def updateThroughput(row, updateDecision):
    global new_dict
    global updated_throughput
    global comm_exceptions
    global active_throughput
    global thru
    logger.info(f"Update? **{updateDecision}**")
    logger.info(f"{row}")
    if updateDecision == "y":
        if cap_override == 'n':
            if active_throughput >= linecapValue:
                active_throughput = linecapValue
            else:
                active_throughput = active_throughput
        if existing in thru:
            existing_dict = existing
            # print('existing = ', existing_dict)
            existing_throughput = existing['cummul_throughput']
            logger.info(f"Existing throughput: {existing_throughput}")
            updated_throughput = existing_throughput + active_throughput
            logger.info(f"Updated throughput: {updated_throughput}")
            existing_dict['cummul_throughput'] = updated_throughput
            #     print('revised through = ', existing_dict)    *********************
            new_dict = replace_values(thru, existing, existing_dict)
            # print('updated throughput for suppliers:', new_dict)
            # print('-----------------------------------------------------')
        else:
            updated_throughput = active_throughput
            # print('Updated throughput cummulative for supplier where no previous existing', updated_throughput)
            cummul.key = 'supplier_id'
            cummul.value = active_supplier
            cummul.add(cummul.key,cummul.value)
            cummul.key1 = 'cummul_throughput'
            cummul.value1 = updated_throughput
            cummul.add(cummul.key1,cummul.value1)
            logger.info(f"New throughput dict added: {cummul}")
            # print(thru)
            thru.append(cummul.copy())
            # print('*+*+*+*+*+Updated cummulated throughput=', thru)
            new_dict = thru
    else:
        print('NO UPDATED NEEEEDEDMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMOOOOOOOOOOOOOOOOOOOOOOOOOOOMMMMMMMMMMMMMMM')
        # updated_throughput = 0
    return [new_dict, updated_throughput]


def updateParentThroughput(nd):
    global parent_dict
    global new_dict
    global final_dict
    global dfParents
    final_dict = nd
    for dic in nd:          # iterates through the current throughput dictionary and pulls out the dictionary for each supplier
        print(dic)
        # tempList = []
        # print(tempList)
        # print('Parent database=', parents)
        supplierInDict = dic['supplier_id']     # gets the supplier_id for the current supplier being reviewed from the throughput dictionary
        throughputinDict = dic['cummul_throughput']   # gets the throughput for the current supplier from the throughput dictionary
        throughput = throughputinDict
        # print(supplierInDict,throughputinDict)
        parentId = next((subDict for subDict in parents if subDict['supplier_id'] == supplierInDict), None)  # gets the current dictionary for the current supplier from the complete parents dictionary
        print('List containing parent ID =', parentId)
        if parentId:
            parentIdinParents = parentId['parent_id']
            print('ParentID =', parentIdinParents)
            #check if there is an existing throughput record for that parent ID in the throughput table
            parentDictInThroughput = next((subDict for subDict in parent_dict if subDict['parent_id'] == parentIdinParents), None)
            print('***ParentDictinThroughput  ', parentDictInThroughput)
            if parentDictInThroughput:
                parentThroughput = parentDictInThroughput['cummul_throughput']
                throughput += parentThroughput
                parentDictInThroughput['cummul_throughput'] = throughput
                # next it loops through the parents dict
                tempList = []
                index = 0
                while index < len(parents):    
                    print('ppppppppppppppppppppppp')
                    print(parents[index])
                    supplierDicPar = parents[index]
                    currParent = supplierDicPar['parent_id']
                    suppForVolUpdate = supplierDicPar['supplier_id']
                    print(suppForVolUpdate)
                    if currParent == parentIdinParents:
                        tempList.insert(1, suppForVolUpdate)
                        print('Templist is', tempList)
                        #check to see that the supplier for the current dict from the parents dict matches the supplier under review 
                        for inL in tempList:    
                            print('dic is = ', dic)
                            throughDict = next((subDict for subDict in final_dict if subDict['supplier_id'] == inL), None)
                            if throughDict:
                                origSupp = throughDict['supplier_id']
                                origThrough = throughDict['cummul_throughput']
                                tempDict.key = 'supplier_id'
                                tempDict.value = origSupp
                                tempDict.add(tempDict.key,tempDict.value)
                                tempDict.key1 = 'cummul_throughput'
                                tempDict.value1 = throughput
                                tempDict.add(tempDict.key1,tempDict.value1)
                                temp_Dict = []
                                temp_Dict.append(tempDict.copy())
                                temp_Dict1 = dict(temp_Dict)
                                print('Gidday  ', temp_Dict1)
                                # print('throughDict=', throughDict)
                                # newThroughDict = throughDict
                                # newThroughDict['cummul_throughput'] = throughput
                                revised_final_dict = replace_values(final_dict, throughDict, temp_Dict1)
                                final_dict = revised_final_dict
                                # newPar.key = 'supplier_id'
                                # newPar.value = suppForVolUpdate
                                # newPar.add(newPar.key,newPar.value)
                                # newPar.key1 = 'cummul_throughput'
                                # newPar.value1 = throughput
                                # newPar.add(newPar.key1,newPar.value1)
                                # final_dict.update({newPar.copy()})
                                print('updated final_dict:', final_dict)
                                # throughDi = next((subD for subD in nd if subD['supplier_id'] == inL), None)
                                # throughDi['cummul_throughput'] = origThrough
                                # print('Dic is still:', dic)
                                # print('final_dict is still:', throughDict)
                    index += 1
                    
            else:
                print('parent supplier does not existing in throughput')
                newPar.key = 'parent_id'
                newPar.value = parentIdinParents
                newPar.add(newPar.key,newPar.value)
                newPar.key1 = 'cummul_throughput'
                newPar.value1 = throughputinDict
                newPar.add(newPar.key1,newPar.value1)
                parent_dict.append(newPar.copy())
                tempList = []
                index = 0
                while index < len(parents):    
                    print('ppppppppppppppppppppppp')
                    print(parents[index])
                    supplierDicPar = parents[index]
                    currParent = supplierDicPar['parent_id']
                    suppForVolUpdate = supplierDicPar['supplier_id']
                    print(suppForVolUpdate)
                    if currParent == parentIdinParents:
                        tempList.insert(1, suppForVolUpdate)
                        print('Templist is', tempList)
                        #check to see that the supplier for the current dict from the parents dict matches the supplier under review 
                        for inL in tempList:    
                            print('dic is = ', dic)
                            throughDict = next((subDict for subDict in final_dict if subDict['supplier_id'] == inL), None)
                            if throughDict:
                                origSupp = throughDict['supplier_id']
                                origThrough = throughDict['cummul_throughput']
                                tempDict.key = 'supplier_id'
                                tempDict.value = origSupp
                                tempDict.add(tempDict.key,tempDict.value)
                                tempDict.key1 = 'cummul_throughput'
                                tempDict.value1 = throughput
                                tempDict.add(tempDict.key1,tempDict.value1)
                                temp_Dict = []
                                temp_Dict.append(tempDict.copy())
                                temp_Dict1 = dict(temp_Dict)
                                print('hello', temp_Dict1)
                                revised_final_dict = replace_values(final_dict, throughDict, temp_Dict1)
                                final_dict = revised_final_dict
                                print('final_dict is          :', final_dict)
                    index +=1

        else:
            print('>>>>>No parent supplier for this supplier')
        # print('Final Throughput AFTER Parent billing review:', nd)
        # print('Parent throughput = ', parent_dict)
        print('----------------------------')
    return[parent_dict, new_dict, final_dict]


def pullRows():
    row = cursor.fetchone()
    while row is not None:
        selectOrderLine(row)
        if active_supplier_type == "PREMIUM":
            logger.info(f"Transaction line: {row}")
            checkExceptions(active_supplier,active_throughput,active_community,active_product_code,active_product)
            calcRate(active_supplier,active_throughput,sp_rate, special_rate_flag, new_dict, zerorate_flag)
            logger.info(f"Line rate is: {line_rate}")
            line_charge = calcCharge(active_supplier,active_throughput,line_rate)
            logger.info(f"Line_charge is: {line_charge}")
            cummulativeCharge(active_supplier, line_charge, zerorate_flag)
            print('------------------------------------------------------------------------------LINE ENDED\n')
            # print('cummalative charges are:', chargesList)
        row = cursor.fetchone()
    
cursor.execute("SELECT * FROM orders_au")
row = cursor.fetchone()
while row is not None:
    selectOrderLine(row)
    if active_supplier_type == "PREMIUM":
        checkExceptions(active_supplier,active_throughput,active_community,active_product_code,active_product)
        updateThroughput(row, updateRequired)
    row = cursor.fetchone()
print('FINAL THROUGHPUT FOR SUPPLIERS: ', new_dict)

updateParentThroughput(new_dict)
print('FINAL THROUGHPUT UPDATED FOR PARENT BILLING', final_dict)
print('PARENT THROUGHPUT', parent_dict)

cursor.execute("SELECT * FROM orders_au")
pullRows()

cursor.execute("TRUNCATE TABLE cThru_au")
cursor.executemany("""
     INSERT INTO cThru_au (supplier_id, cummul_throughput) VALUES (%(supplier_id)s, %(cummul_throughput)s)
     """, new_dict)

new_dict = []


cursor.execute("TRUNCATE TABLE totalCharges_au")
print ('***CHARGES:', chargesList)


cursor.executemany("""
     INSERT INTO totalCharges_au (supplier_id, cummul_fees, min_fee) VALUES (%(supplier_id)s, %(cummul_fees)s, %(min_fee)s)
     """, chargesList)



connection.commit


cursor.execute("SELECT * from cThru_au")
thru = cursor.fetchall()
print('Revised Database: ', thru)

thru = []

connection.commit()

connection.close()

