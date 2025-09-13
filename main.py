import pymongo,requests,os,threading
from urllib.parse import urlparse

mongodb = os.environ.get("MONGODB_URI")

data = []
new_data = []
purl_netloc_list = []
netloc_frelist = []
acount = 0

def count_frequency(arr):
    frequency = {}
    for item in arr:
        if item in frequency:
            frequency[item] += 1
        else:
            frequency[item] = 1
    return frequency

def req(url):
    global acount
    headers = {
        'User-Agent': 'CronRequest-service/1.0'
    }
    res = requests.get(url, headers=headers, timeout=60)
    if res.text != "ok":
        return False
    return True

def start():
    global data, new_data, netloc_frelist, acount
    while True:
        try:
            i = data.pop(0)
        except Exception:
            break
        acount += 1
        if netloc_frelist[urlparse(i["purl"]).netloc] <= 2 and i["status"] != "banned" and (i["tick_time"] * 5) >= i["time"]:
            try:
                if req(i["purl"]) == False:
                    raise Exception("banned")
            except Exception:
                i["status"] = "banned"

            i["tick_time"] = 0
            i["times"] += 1

        new_data.append(i)

def main():
    global data, new_data, purl_netloc_list, netloc_frelist, acount
    
    client = pymongo.MongoClient(mongodb)
    db = client["main"]
    collection = db["main"]

    r_data = collection.find()

    for i in r_data:
        a = i
        a.pop("email")
        a["time"] = int(a["time"])
        a["tick_time"] = int(a["tick_time"])
        a["times"] = int(a["times"])
        data.append(a)

    for i in data:
        netloc = urlparse(i["purl"]).netloc
        purl_netloc_list.append(netloc)
        i["tick_time"] += 1

    netloc_frelist = count_frequency(purl_netloc_list)

    threads = []
    for i in range(200):
        thread = threading.Thread(target=start, args=())
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

    if(len(data) == 0):
        print("Count:",acount)
        for i in new_data:
            #print(i)
            collection.update_one({"_id": i["_id"]}, {"$set": i})    

        client.close()
        return




main()


