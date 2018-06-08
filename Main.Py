import requests
import numpy as np
from datetime import datetime
import time
import pymysql.cursors
from threading import Thread, Lock

connection_db = pymysql.connect(host='localhost',
                                user='root',
                                port=3306,
                                password='',
                                db='test')

cursor_db = connection_db.cursor()
AliveQuery = "SET SESSION interactive_timeout = 3600"
AliveQuery2 = "SET SESSION wait_timeout = 3600"
cursor_db.execute(AliveQuery)
cursor_db.execute(AliveQuery2)

lock = Lock()

btc_list = ['BTC-LTC', 'BTC-DOGE', 'BTC-VTC', 'BTC-PPC', 'BTC-FTC', 'BTC-RDD', 'BTC-NXT', 'BTC-DASH', 'BTC-POT',
            'BTC-BLK', 'BTC-EMC2', 'BTC-XMY', 'BTC-AUR', 'BTC-EFL', 'BTC-GLD', 'BTC-SLR', 'BTC-PTC', 'BTC-GRS',
            'BTC-NLG', 'BTC-RBY', 'BTC-XWC', 'BTC-MONA', 'BTC-THC', 'BTC-ENRG', 'BTC-ERC', 'BTC-VRC', 'BTC-CURE',
            'BTC-XMR', 'BTC-CLOAK', 'BTC-KORE', 'BTC-XDN', 'BTC-TRUST', 'BTC-NAV', 'BTC-XST', 'BTC-VIA', 'BTC-PINK',
            'BTC-IOC', 'BTC-CANN', 'BTC-SYS', 'BTC-NEOS', 'BTC-DGB', 'BTC-BURST', 'BTC-EXCL', 'BTC-SWIFT', 'BTC-DOPE',
            'BTC-BLOCK', 'BTC-ABY', 'BTC-BYC', 'BTC-XMG', 'BTC-BLITZ', 'BTC-BAY', 'BTC-SPR', 'BTC-VTR', 'BTC-XRP',
            'BTC-GAME', 'BTC-COVAL', 'BTC-NXS', 'BTC-XCP', 'BTC-BITB', 'BTC-GEO', 'BTC-FLDC', 'BTC-GRC', 'BTC-FLO',
            'BTC-NBT', 'BTC-MUE', 'BTC-XEM', 'BTC-CLAM', 'BTC-DMD', 'BTC-GAM', 'BTC-SPHR', 'BTC-OK', 'BTC-SNRG',
            'BTC-PKB', 'BTC-AEON', 'BTC-ETH', 'BTC-TX', 'BTC-BCY', 'BTC-EXP', 'BTC-OMNI', 'BTC-AMP', 'BTC-XLM',
            'BTC-RVR', 'BTC-EMC', 'BTC-FCT', 'BTC-EGC', 'BTC-SLS', 'BTC-RADS', 'BTC-DCR', 'BTC-BSD', 'BTC-XVG',
            'BTC-PIVX', 'BTC-MEME', 'BTC-STEEM', 'BTC-2GIVE', 'BTC-LSK', 'BTC-BRK', 'BTC-WAVES', 'BTC-LBC', 'BTC-SBD',
            'BTC-BRX', 'BTC-ETC', 'BTC-STRAT', 'BTC-UNB', 'BTC-SYNX', 'BTC-EBST', 'BTC-VRM', 'BTC-SEQ', 'BTC-REP',
            'BTC-SHIFT', 'BTC-ARDR', 'BTC-XZC', 'BTC-NEO', 'BTC-ZEC', 'BTC-ZCL', 'BTC-IOP', 'BTC-GOLOS', 'BTC-UBQ',
            'BTC-KMD', 'BTC-GBG', 'BTC-SIB', 'BTC-ION', 'BTC-LMC', 'BTC-QWARK', 'BTC-CRW', 'BTC-SWT', 'BTC-MLN',
            'BTC-ARK', 'BTC-DYN', 'BTC-TKS', 'BTC-MUSIC', 'BTC-DTB', 'BTC-INCNT', 'BTC-GBYTE', 'BTC-GNT', 'BTC-NXC',
            'BTC-EDG', 'BTC-LGD', 'BTC-TRST', 'BTC-WINGS', 'BTC-RLC', 'BTC-GNO', 'BTC-GUP', 'BTC-LUN', 'BTC-HMQ',
            'BTC-ANT', 'BTC-SC', 'BTC-BAT', 'BTC-ZEN', 'BTC-QRL', 'BTC-CRB', 'BTC-PTOY', 'BTC-CFI', 'BTC-BNT',
            'BTC-NMR', 'BTC-SNT', 'BTC-DCT', 'BTC-XEL', 'BTC-MCO', 'BTC-ADT', 'BTC-PAY', 'BTC-STORJ', 'BTC-ADX',
            'BTC-OMG', 'BTC-CVC', 'BTC-PART', 'BTC-QTUM', 'BTC-BCC', 'BTC-DNT', 'BTC-ADA', 'BTC-MANA', 'BTC-SALT',
            'BTC-TIX', 'BTC-RCN', 'BTC-VIB', 'BTC-MER', 'BTC-POWR', 'BTC-BTG', 'BTC-ENG', 'BTC-UKG', 'BTC-IGNIS',
            'BTC-SRN', 'BTC-WAX', 'BTC-ZRX', 'BTC-VEE', 'BTC-BCPT', 'BTC-TRX', 'BTC-TUSD', 'BTC-LRC', 'BTC-UP',
            'BTC-DMT']
eth_list = ['ETH-ETC', 'ETH-GNT', 'ETH-REP', 'ETH-WINGS', 'ETH-GUP', 'ETH-RLC', 'ETH-LUN', 'ETH-GNO', 'ETH-HMQ',
            'ETH-TRST', 'ETH-ANT', 'ETH-BAT', 'ETH-QRL', 'ETH-CRB', 'ETH-LGD', 'ETH-PTOY', 'ETH-CFI', 'ETH-BNT',
            'ETH-NMR', 'ETH-LTC', 'ETH-XRP', 'ETH-SNT', 'ETH-MCO', 'ETH-ADT', 'ETH-PAY', 'ETH-STORJ', 'ETH-ADX',
            'ETH-DASH', 'ETH-SC', 'ETH-ZEC', 'ETH-OMG', 'ETH-CVC', 'ETH-QTUM', 'ETH-XMR', 'ETH-XEM', 'ETH-XLM',
            'ETH-NEO', 'ETH-BCC', 'ETH-DNT', 'ETH-WAVES', 'ETH-STRAT', 'ETH-DGB', 'ETH-FCT', 'ETH-MANA', 'ETH-SALT',
            'ETH-TIX', 'ETH-RCN', 'ETH-VIB', 'ETH-POWR', 'ETH-BTG', 'ETH-ADA', 'ETH-ENG', 'ETH-UKG', 'ETH-SRN',
            'ETH-WAX', 'ETH-ZRX', 'ETH-VEE', 'ETH-BCPT', 'ETH-TRX', 'ETH-LRC', 'ETH-TUSD', 'ETH-UP', 'ETH-DMT']
usdt_list = ['USDT-BTC', 'USDT-ETH', 'USDT-ZEC', 'USDT-LTC', 'USDT-ETC', 'USDT-XRP', 'USDT-XMR', 'USDT-DASH',
             'USDT-BCC', 'USDT-NEO', 'USDT-OMG', 'USDT-BTG', 'USDT-ADA', 'USDT-XVG', 'USDT-NXT', 'USDT-TUSD']

cur_list = ['BTC-LTC', 'BTC-DOGE', 'BTC-VTC', 'BTC-PPC', 'BTC-FTC', 'BTC-RDD', 'BTC-NXT', 'BTC-DASH', 'BTC-POT',
            'BTC-BLK', 'BTC-EMC2', 'BTC-XMY', 'BTC-AUR', 'BTC-EFL', 'BTC-GLD', 'BTC-SLR', 'BTC-PTC', 'BTC-GRS',
            'BTC-NLG', 'BTC-RBY', 'BTC-XWC', 'BTC-MONA', 'BTC-THC', 'BTC-ENRG', 'BTC-ERC', 'BTC-VRC', 'BTC-CURE',
            'BTC-XMR', 'BTC-CLOAK', 'BTC-KORE', 'BTC-XDN', 'BTC-TRUST', 'BTC-NAV', 'BTC-XST', 'BTC-VIA', 'BTC-PINK',
            'BTC-IOC', 'BTC-CANN', 'BTC-SYS', 'BTC-NEOS', 'BTC-DGB', 'BTC-BURST', 'BTC-EXCL', 'BTC-SWIFT', 'BTC-DOPE',
            'BTC-BLOCK', 'BTC-ABY', 'BTC-BYC', 'BTC-XMG', 'BTC-BLITZ', 'BTC-BAY', 'BTC-SPR', 'BTC-VTR', 'BTC-XRP',
            'BTC-GAME', 'BTC-COVAL', 'BTC-NXS', 'BTC-XCP', 'BTC-BITB', 'BTC-GEO', 'BTC-FLDC', 'BTC-GRC', 'BTC-FLO',
            'BTC-NBT', 'BTC-MUE', 'BTC-XEM', 'BTC-CLAM', 'BTC-DMD', 'BTC-GAM', 'BTC-SPHR', 'BTC-OK', 'BTC-SNRG',
            'BTC-PKB', 'BTC-AEON', 'BTC-ETH', 'BTC-TX', 'BTC-BCY', 'BTC-EXP', 'BTC-OMNI', 'BTC-AMP', 'BTC-XLM',
            'USDT-BTC', 'BTC-RVR', 'BTC-EMC', 'BTC-FCT', 'BTC-EGC', 'BTC-SLS', 'BTC-RADS', 'BTC-DCR', 'BTC-BSD',
            'BTC-XVG', 'BTC-PIVX', 'BTC-MEME', 'BTC-STEEM', 'BTC-2GIVE', 'BTC-LSK', 'BTC-BRK', 'BTC-WAVES', 'BTC-LBC',
            'BTC-SBD', 'BTC-BRX', 'BTC-ETC', 'ETH-ETC', 'BTC-STRAT', 'BTC-UNB', 'BTC-SYNX', 'BTC-EBST', 'BTC-VRM',
            'BTC-SEQ', 'BTC-REP', 'BTC-SHIFT', 'BTC-ARDR', 'BTC-XZC', 'BTC-NEO', 'BTC-ZEC', 'BTC-ZCL', 'BTC-IOP',
            'BTC-GOLOS', 'BTC-UBQ', 'BTC-KMD', 'BTC-GBG', 'BTC-SIB', 'BTC-ION', 'BTC-LMC', 'BTC-QWARK', 'BTC-CRW',
            'BTC-SWT', 'BTC-MLN', 'BTC-ARK', 'BTC-DYN', 'BTC-TKS', 'BTC-MUSIC', 'BTC-DTB', 'BTC-INCNT', 'BTC-GBYTE',
            'BTC-GNT', 'BTC-NXC', 'BTC-EDG', 'BTC-LGD', 'BTC-TRST', 'ETH-GNT', 'ETH-REP', 'USDT-ETH', 'ETH-WINGS',
            'BTC-WINGS', 'BTC-RLC', 'BTC-GNO', 'BTC-GUP', 'BTC-LUN', 'ETH-GUP', 'ETH-RLC', 'ETH-LUN', 'ETH-GNO',
            'BTC-HMQ', 'ETH-HMQ', 'BTC-ANT', 'ETH-TRST', 'ETH-ANT', 'BTC-SC', 'ETH-BAT', 'BTC-BAT', 'BTC-ZEN',
            'BTC-QRL', 'ETH-QRL', 'BTC-CRB', 'ETH-CRB', 'ETH-LGD', 'BTC-PTOY', 'ETH-PTOY', 'BTC-CFI', 'ETH-CFI',
            'BTC-BNT', 'ETH-BNT', 'BTC-NMR', 'ETH-NMR', 'ETH-LTC', 'ETH-XRP', 'BTC-SNT', 'ETH-SNT', 'BTC-DCT',
            'BTC-XEL', 'BTC-MCO', 'ETH-MCO', 'BTC-ADT', 'ETH-ADT', 'BTC-PAY', 'ETH-PAY', 'BTC-STORJ', 'ETH-STORJ',
            'BTC-ADX', 'ETH-ADX', 'ETH-DASH', 'ETH-SC', 'ETH-ZEC', 'USDT-ZEC', 'USDT-LTC', 'USDT-ETC', 'USDT-XRP',
            'BTC-OMG', 'ETH-OMG', 'BTC-CVC', 'ETH-CVC', 'BTC-PART', 'BTC-QTUM', 'ETH-QTUM', 'ETH-XMR', 'ETH-XEM',
            'ETH-XLM', 'ETH-NEO', 'USDT-XMR', 'USDT-DASH', 'ETH-BCC', 'USDT-BCC', 'BTC-BCC', 'BTC-DNT', 'ETH-DNT',
            'USDT-NEO', 'ETH-WAVES', 'ETH-STRAT', 'ETH-DGB', 'ETH-FCT', 'USDT-OMG', 'BTC-ADA', 'BTC-MANA', 'ETH-MANA',
            'BTC-SALT', 'ETH-SALT', 'BTC-TIX', 'ETH-TIX', 'BTC-RCN', 'ETH-RCN', 'BTC-VIB', 'ETH-VIB', 'BTC-MER',
            'BTC-POWR', 'ETH-POWR', 'BTC-BTG', 'ETH-BTG', 'USDT-BTG', 'ETH-ADA', 'BTC-ENG', 'ETH-ENG', 'USDT-ADA',
            'USDT-XVG', 'USDT-NXT', 'BTC-UKG', 'ETH-UKG', 'BTC-IGNIS', 'BTC-SRN', 'ETH-SRN', 'BTC-WAX', 'ETH-WAX',
            'BTC-ZRX', 'ETH-ZRX', 'BTC-VEE', 'ETH-VEE', 'BTC-BCPT', 'ETH-BCPT', 'BTC-TRX', 'ETH-TRX', 'BTC-TUSD',
            'BTC-LRC', 'ETH-LRC', 'ETH-TUSD', 'BTC-UP', 'ETH-UP', 'BTC-DMT', 'ETH-DMT', 'USDT-TUSD']
threads = []


def bitfunc(Pair, connection, cursor):
 
    Pairslice = Pair.replace("-", "")
    x = 0
    Id_arr = np.array([])
    time_stamp_arr = np.array([])
    price_arr = np.array([])
    vol_arr = np.array([])
    BaseVol_arr = np.array([])

    url = 'https://bittrex.com/api/v1.1/public/getmarkethistory?market=' + Pair
    data = requests.get(url).json()
    data['result'].reverse()
    with lock:
        sqlQuery = "CREATE TABLE IF NOT EXISTS " + Pairslice + "(Id INT, TimeStamp TIMESTAMP(3), Quantity DECIMAL(18,10), Price DECIMAL(18,10), Total DECIMAL(18,10), FillType VARCHAR(15), OrderType VARCHAR(15))"
        cursor.execute(sqlQuery)

    for i in data['result']:
        Id = i['Id']
        Id_arr = np.append(Id_arr, Id)
        time_stamp = i['TimeStamp']
        time_stamp_arr = np.append(time_stamp_arr, time_stamp)
        vol = i['Quantity']
        vol_arr = np.append(vol_arr, vol)
        price = i['Price']
        price_arr = np.append(price_arr, price)
        BaseVol = i['Total']
        BaseVol_arr = np.append(BaseVol_arr, BaseVol)
        total = i['Total']
        Fill_Type = i['FillType']
        Order_Type = i['OrderType']
    with lock:
        Id = i['Id']
        time_stamp = i['TimeStamp']
        vol = i['Quantity']
        price = i['Price']
        total = i['Total']
        Fill_Type = i['FillType']
        Order_Type = i['OrderType']
        addQuery = "INSERT INTO " + Pairslice + "(`Id`, `Quantity`, `TimeStamp`, `Price`, `Total`, `FillType`, `OrderType`) VALUES ( %s, %s, TIMESTAMP(%s), %s, %s, %s, %s);"
        cursor.execute(addQuery,
                       (str(Id), str(vol), str(time_stamp), str(price), str(total), str(Fill_Type), str(Order_Type)))
        connection.commit()

    for j in time_stamp_arr:
        print(j)

    while True:
        time_now = datetime.now()
        second_now = time_now.second
        if second_now == 20:
            print(" ")
            print(time_now)
            print("********************LOOP OPENED FOR: " + Pair + "***********************")
            url = 'https://bittrex.com/api/v1.1/public/getmarkethistory?market=' + Pair
            data = requests.get(url).json()
            data['result'].reverse()
            count = 0

            for i in data['result']:
                time_stamp = i['TimeStamp']
                Id = i['Id']
                vol = i['Quantity']
                price = i['Price']
                BaseVol = i['Total']
                if np.datetime64(time_stamp_arr[-1]) > np.datetime64(time_stamp):
                    pass
                elif np.datetime64(time_stamp_arr[-1]) == np.datetime64(time_stamp):
                    if Id_arr[-1] == Id or Id_arr[-2] == Id or Id_arr[-3] == Id or Id_arr[-4] == Id:
                        pass
                    else:
                        if Id_arr.size > 100:
                            Id_arr = np.delete(Id_arr, np.s_[:-100], 0)
                            time_stamp_arr = np.delete(time_stamp_arr, np.s_[:-100], 0)
                            vol_arr = np.delete(vol_arr, np.s_[:-100], 0)
                            price_arr = np.delete(price_arr, np.s_[:100], 0)
                            BaseVol_arr = np.delete(BaseVol_arr, np.s_[:-100], 0)

                            Id_arr = np.append(Id_arr, Id)
                            time_stamp_arr = np.append(time_stamp_arr, time_stamp)
                            vol_arr = np.append(vol_arr, vol)
                            price_arr = np.append(price_arr, price)
                            BaseVol_arr = np.append(BaseVol_arr, BaseVol)
                        else:
                            Id_arr = np.append(Id_arr, Id)
                            time_stamp_arr = np.append(time_stamp_arr, time_stamp)
                            vol_arr = np.append(vol_arr, vol)
                            price_arr = np.append(price_arr, price)
                            BaseVol_arr = np.append(BaseVol_arr, BaseVol)

                        total = i['Total']
                        Fill_Type = i['FillType']
                        Order_Type = i['OrderType']
                        with lock:
                            addQuery = "INSERT INTO " + Pairslice + "(`Id`, `Quantity`, `TimeStamp`, `Price`, `Total`, `FillType`, `OrderType`) VALUES ( %s, %s, TIMESTAMP(%s), %s, %s, %s, %s);"
                            cursor.execute(addQuery, (
                            str(Id), str(vol), str(time_stamp), str(price), str(total), str(Fill_Type),
                            str(Order_Type)))
                            connection.commit()
                        print(Pair +" ==> "+str(time_stamp))
                        count += 1
                elif np.datetime64(time_stamp_arr[-1]) < np.datetime64(time_stamp):
                    if Id_arr.size > 100:
                        Id_arr = np.delete(Id_arr, np.s_[:-100], 0)
                        time_stamp_arr = np.delete(time_stamp_arr, np.s_[:-100], 0)
                        vol_arr = np.delete(vol_arr, np.s_[:-100], 0)
                        price_arr = np.delete(price_arr, np.s_[:100], 0)
                        BaseVol_arr = np.delete(BaseVol_arr, np.s_[:-100], 0)

                        Id_arr = np.append(Id_arr, Id)
                        time_stamp_arr = np.append(time_stamp_arr, time_stamp)
                        vol_arr = np.append(vol_arr, vol)
                        price_arr = np.append(price_arr, price)
                        BaseVol_arr = np.append(BaseVol_arr, BaseVol)
                    else:
                        Id_arr = np.append(Id_arr, Id)
                        time_stamp_arr = np.append(time_stamp_arr, time_stamp)
                        vol_arr = np.append(vol_arr, vol)
                        price_arr = np.append(price_arr, price)
                        BaseVol_arr = np.append(BaseVol_arr, BaseVol)

                    total = i['Total']
                    Fill_Type = i['FillType']
                    Order_Type = i['OrderType']
                    with lock:
                        addQuery = "INSERT INTO " + Pairslice + "(`Id`, `Quantity`, `TimeStamp`, `Price`, `Total`, `FillType`, `OrderType`) VALUES ( %s, %s, TIMESTAMP(%s), %s, %s, %s, %s);"
                        cursor.execute(addQuery, (
                            str(Id), str(vol), str(time_stamp), str(price), str(total), str(Fill_Type),
                            str(Order_Type)))
                        connection.commit()
                    print(Pair+" ==> "+time_stamp)
                    count += 1
                else:
                    print("something is wrong dude")
            print("*****************LOOP CLOSED FOR: " + Pair + "*******************")
            print(" ")
            print(" ")
        else:
            pass
        time.sleep(1)


for i in cur_list:
    thread = Thread(target=bitfunc, args=(i, connection_db, cursor_db), daemon=True)
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
