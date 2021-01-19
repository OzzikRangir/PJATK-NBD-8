import riak

if __name__ == "__main__":
    myClient = riak.RiakClient()
    myBucket = myClient.bucket('s22010')

    val1 = {"name" : "Vodka", "type": "drink", "weight": 0.7, "price": 21.37}

    key1 = myBucket.new('vodka', data=val1)
    key1.store()
    print("STORED key vodka: " + str(val1))

    fetched1 = myBucket.get('vodka')
    print ("GET key vodka: "+str(fetched1.data))

    fetched1.data['prize'] = 32.80
    fetched1.store()
    print ("UPDATE key vodka: "+str(fetched1.data))

    fetched1 = myBucket.get('vodka')
    print ("GET key vodka: "+str(fetched1.data))

    fetched1.delete()
    print ("DELETE key vodka")

    fetched1 = myBucket.get('vodka')
    print ("GET key vodka: "+str(fetched1.data))
