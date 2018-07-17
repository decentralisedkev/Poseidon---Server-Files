import requests
import json
import boto3
import sys
from boto3.dynamodb.conditions import Key
import random

file = "/home/ec2-user/Projects/src/processBlocks/utxoUpdatecounter.txt"
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

nodes = [
    "http://test4.cityofzion.io:8880",
    "http://test3.cityofzion.io:8880",
    "http://seed3.neo.org:20332",
    "http://test2.cityofzion.io:8880",
    "http://test1.cityofzion.io:8880",
    "http://test5.cityofzion.io:8880",
    "https://seed1.neo.org:20331",
    "http://seed5.neo.org:20332",
    "http://seed2.neo.org:20332",
    "http://seed4.neo.org:20332",]

def FileSave(filename,content):
    with open(filename, "a") as myfile:
        myfile.write(content)

def processVouts(vouts, transaction,blockNumber):
    table = dynamodb.Table('Test-Outputs')
    with table.batch_writer(overwrite_by_pkeys=['Address', 'TxID_N']) as batch:
        transactionType = transaction['type']    	    
        for vout in vouts:
            address = vout['address']
            asset = vout['asset']
            value = vout['value']
            txid_n = transaction['txid'] +"_"+ str(vout['n'])
            batch.put_item(
                Item={
                    'Address': address,
                    'TxID_N': txid_n,
                    'Asset': asset,
                    'Value': value,
                    'Start':blockNumber,
                    'Type' : transactionType
                }
            )
def processVins(vins,transaction,blockNumber):
    table = dynamodb.Table('Test-Inputs')
    with table.batch_writer(overwrite_by_pkeys=['TxID_N', 'BlockNumber']) as batch:
        for vin in vins:
            txid_n = vin['txid'] +"_"+str(vin['vout'])
            txidn = txid_n
            #txidn, blocknum, txidCurrent
            batch.put_item(
                Item={
			        'BlockNumber': blockNumber,
			        'TxID_N':txid_n,
			        'TXID': transaction['txid'],
			    }
            )		
            
def getAndProcessBlock(blockNumber, nodeUrl, retries):  
    url = nodeUrl
    
    headers = {'content-type': 'application/json'}
    payload = {
        "method": "getblock",
        "params": [blockNumber,1],
        "jsonrpc": "2.0",
        "id": 0,
    }

    response = requests.post(url, data=json.dumps(payload), headers=headers).json()
    
    if 'result' in response:
        transactions = response['result']['tx']
        if len(transactions) < 2:
            num = blockNumber + 1
            f = open(file, 'w')
            f.write(str(num))
            return
        for transaction in transactions:
            vouts = transaction['vout']
            vins = transaction['vin']
            
            processVouts(vouts,transaction,blockNumber)
            processVins(vins, transaction ,blockNumber)
        
        num = blockNumber+1
        f = open(file, 'w')
        f.write(str(num))
    else:
        if retries <= len(nodes):
            retries = retries + 1
            getAndProcessBlock(blockNumber, random.choice(nodes), retries)
        else:
            
            FileSave("BlockErrorLog.txt", str(blockNumber)+"\n")
            print("ERROR: Block not processed", blockNumber)

if __name__ == "__main__":


    f = open(file, 'r+')
    text = f.read()
    num = int(text)

    for i in range(num,num+644): #[a,b) lower should be 1 upper should be 3 to process 2 blocks
        print(i) 
        getAndProcessBlock(i, random.choice(nodes),0)
    #print("Done")


