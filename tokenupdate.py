import boto3
import json
import sys
import requests
dynamo = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamo.Table('Test-TokenTable')
file = '/home/ec2-user/Projects/src/processBlocks/transferCounter.txt'
def processJson(results):
    with table.batch_writer(overwrite_by_pkeys=['Address', 'Script_TxID']) as batch:
        for jsn in results:
            if 'amount' not in jsn:
                continue
            val = str(jsn['amount'])
            fromAddr = "empty"
            if 'addr_from' in jsn:
                fromAddr=jsn['addr_from']
            toAddr = jsn['addr_to']
            scriptHash = jsn['contract']
            
            
            txid = jsn['tx']
#            print(txid)
            blockindex = jsn['block']
            if fromAddr == "AFmseVrdL9f9oyCzZefL9tG6UbvhPbdYzM" or (fromAddr == "" and 'addr_from' in jsn):
                batch.put_item(
                    Item={
                        'Address': toAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': val,
                        'BlockNumber': blockindex,
                        'Type': "deploy",
                    }

                )
            elif jsn['notify_type'] == "refund":
                batch.put_item(
                    Item={
                        'Address': toAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': val,
                        'BlockNumber': blockindex,
                        'Type': "mint",
                    }
                )
            
            elif jsn['notify_type'] == "transfer":
                batch.put_item(
                    Item={
                        'Address': toAddr,
                        'From': fromAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': val,
                        'BlockNumber': blockindex,
                        'Type': "received",
                    }
                )
                batch.put_item(
                    Item={
                        'Address': fromAddr,
                        'To': toAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': '-'+val,
                        'BlockNumber': blockindex,
                        'Type': "sent",
                    }
                )

            elif jsn['notify_type'] == "approve":
                batch.put_item(
                    Item={
                        'Address': toAddr,
                        'From': fromAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': val,
                        'BlockNumber': blockindex,
                        'Type': "approve",
                    }
                )
                batch.put_item(
                    Item={
                        'Address': fromAddr,
                        'To': toAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': '-'+val,
                        'BlockNumber': blockindex,
                        'Type': "approve",
                    }
                )

            elif jsn['notify_type'] == "mint":
                batch.put_item(
                    Item={
                        'Address': toAddr,
                        'From': fromAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': val,
                        'BlockNumber': blockindex,
                        'Type': "mint",
                    }
                )
                batch.put_item(
                    Item={
                        'Address': fromAddr,
                        'From': toAddr,
                        'Script_TxID':scriptHash+"_"+txid,
                        'Value': '-'+val,
                        'BlockNumber': blockindex,
                        'Type': "mint",
                    }
                )
def getResp(blockNumber, pageNumber):
    url = 'http://testnotifications.neeeo.org/v1/notifications/block/'+ str(blockNumber) #+text
    params = dict(
        page=pageNumber,
    )
    resp = requests.get(url=url, params=params)
    # print("Response", resp.json())
    data = resp.json()
    #Check the next page to see if there are results
    params = dict(
        page=pageNumber+1,
    )

    resp = requests.get(url=url, params=params)

    moreRes = resp.json()['results']
  
    if len(moreRes) > 1:
        return (True,data)
    else:
        return (False, data)
        
    

if __name__ == "__main__":
    f = open(file, 'r+')
    text = f.read()
    num = int(text)
    results = []
    pageNumber = 1
    
    for i in range(num,num+200): #[a,b)
        pageNumber = 1
        moreResults = True
        while moreResults:    
            moreResults, result = getResp(i,pageNumber)  
            pageNumber=pageNumber+1
            results.append(result)
       # print(i)
        hasResults = False
        for element in results:
            hasResults = True
                # print(element['results'])
            processJson(element['results'])
        newNum = i
        if i > results[0]['current_height']:
            newNum = results[0]['current_height']
        f = open(file, 'w')
        f.write(str(newNum))
                

