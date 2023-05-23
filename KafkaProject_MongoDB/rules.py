from geo_map import GEO_Map
from pymongo import MongoClient
from datetime import datetime

# 900km/h: 900/3600 = 0.25 km/s
speed_limit = 0.25 

def speed_calc(dist, trans_dt, last_trans_dt):
    trans_dt      = datetime.strptime(trans_dt,      '%d-%m-%Y %H:%M:%S')
    last_trans_dt = datetime.strptime(last_trans_dt, '%d-%m-%Y %H:%M:%S')
    consumed_time = (trans_dt - last_trans_dt).total_seconds()

    try:
        return dist / consumed_time
    except ZeroDivisionError:
        return 299792.458


def verify_postcode(card_id, postcode, trans_dt):
    try:
        geo_map = GEO_Map.get_instance()
        conn = MongoClient();
        db = conn["transaction_db"]
        lookupTable = db["tb_lookup"]
        card_info = lookupTable.find_one({'card_id': card_id})
        last_postcode = card_info['postcode']
        last_trans_dt = card_info['transaction_dt']

        current_lat = geo_map.get_lat(str(postcode))
        for data in current_lat:
            current_lat1 = data
        current_long = geo_map.get_long(str(postcode))
        for data in current_long:
            current_long1 = data
        previous_lat = geo_map.get_lat(str(last_postcode))
        for data in previous_lat:
            previous_lat1 = data
        previous_long = geo_map.get_long(str(last_postcode))
        for data in previous_long:
            previous_long1 = data
        
        distance = geo_map.distance(lat1=current_lat1, long1=current_long1, lat2=previous_lat1, long2=previous_long1)

        
        veloc = speed_calc(distance, trans_dt, last_trans_dt)

        if veloc < speed_limit:
            return True
        else:
            return False
    except Exception as e:
        raise Exception(e)


def verify_ucl(card_id, amount):
    try:
        conn = MongoClient();
        db = conn["transaction_db"]
        lookupTable = db["tb_lookup"]
        card_info = lookupTable.find_one({'card_id': card_id})
        
        if amount < float(card_info["ucl"]):
            return True
        else:
            return False
    except Exception as e:
        raise Exception(e)


def verify_score(card_id):
    try:
        conn = MongoClient();
        db = conn["transaction_db"]
        lookupTable = db["tb_lookup"]
        card_info = lookupTable.find_one({'card_id': card_id})
        
        if int(card_info["score"]) > 200:
            return True
        else:
            return False
    except Exception as e:
        raise Exception(e)


def verify_rules(card_id, amount, postcode, transaction_dt):
    rule_ucl      = verify_ucl(card_id, amount)
    rule_score    = verify_score(card_id)
    rule_postcode = verify_postcode(card_id, postcode, transaction_dt)

    if (rule_ucl==True and rule_score==True and rule_postcode==True):
        return True
    else : 
        return False