######
from flask import Flask, request, render_template
import redis
import json

######################
# For Local development...
# docker create --name local-redis -p 6379:6379  redis:latest
# For Docker...
# docker network create my-net
# docker create --name my-redis --network my-net -p 6379:6379  redis:latest
# docker create --name my-www   --network my-net -p 5000:5000  myweb:latest
######################
app = Flask(__name__)
######################
myRedis = None
######################
host = None
if __name__ == "__main__":
    host = "localhost"
elif __name__ == "app":
    host = "my-redis"
else:
    raise ValueError('Invalid "__name__ variable!')

######################
def InitRedisDatabase():

    # Your data goes here...

    global myRedis
    myRedis = redis.Redis(host=host, port=6379, db=0)
    # myRedis.set(..., json.dumps(...))


######################
@app.route("/data", methods=["GET"])
def GetData():
    if request.method == "GET":
        key = request.args.get("key")
        print("Key:", key)
        if key:
            data = myRedis.get(key)
            print("Data:", data)
            if data:
                return json.loads(data)
            else:
                return '{"Error":"Key could not be found!"}'


######################

InitRedisDatabase()
print(__name__)
if __name__ == "__main__":
    app.run(debug=True, host=host, port=5000)
