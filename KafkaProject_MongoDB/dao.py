from pymongo import MongoClient

def insertCardTrans(trans, Status):
    conn = MongoClient();
    db = conn["transaction_db"]
    coll = db["card_transactions"]

    if (Status==True):
        val_Status = "GENUINE"
    else:
        val_Status = "FRAUD"
        
    doc = {"card_id" : trans["card_id"], 
           "member_id" : trans["member_id"], 
           "amount" : trans["amount"], 
           "postcode" : trans["postcode"], 
           "pos_id" : trans["pos_id"], 
           "transaction_dt" : trans["transaction_dt"], 
           "status" : val_Status}    
    coll.insert_one(doc)


def updateLookup(trans):
    conn = MongoClient();
    db = conn["transaction_db"]
    coll = db["tb_lookup"]

    condition = {"card_id" : trans["card_id"]}    
    newDoc = {"$set": {"postcode" : trans["postcode"], 
                       "transaction_dt" : trans["transaction_dt"]}}
    coll.update_one(condition, newDoc)