import logging
import redis
import time
#docker pull redis
#docker run --name myredis -p 6379:6379 -d redis
def main():

    ttl = 50
    key = "vb#1"

    vitamin = {
        "Vendor": "Now Foods",
        "Title": "Now Foods Thiamine B1 Energy Support",
        "Description": "Vitamin B1, Called Thiamine, C12H17N4OS+",
        "PrimaryUnits": "Pills",
        "PrimaryUnitsPerBottle": 100,
        "DoseUnit": "mg",
        "DosePerPrimaryUnit": 100,
        "Price Units": "USD",
        "Price": 9.99,
        "Country Sold in": "Worldwide",
    }

    myRedis = redis.Redis(host="localhost",port=6379, db=0)
    myRedis.hset(key, mapping=vitamin)

    myRedis.expire(key, ttl)
    time.sleep(5)
    time_remaining=myRedis.ttl(key)
    print(time_remaining)

    try:
        d1=myRedis.hget(key, 'Price')
        print(d1)

        d2=myRedis.hkeys(key)
        print(d2)

        d3=myRedis.hvals(key)
        print(d3)

        d4=myRedis.hgetall(key)
        print(d4)

        myRedis.flushall()

    except redis.RedisError as e:
        logging.error(e)


main()

