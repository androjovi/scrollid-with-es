import os, sys, gc, warnings, time, multiprocessing, argparse
from elasticsearch import Elasticsearch as es
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

load_dotenv()

client = es(hosts=os.environ.get('HOST_ELASTICSEARCH'), basic_auth=(os.environ.get('ELASTICSEARCH_BASICAUTH_USERNAME'), os.environ.get('ELASTICSEARCH_BASICAUTH_PASSWORD')), verify_certs=False, timeout=180, max_retries=10, retry_on_timeout=True)

def getdata(idx, filename, size):
	print(f"Create scroll index for index {idx}, size {size}")
	response = client.search(index=idx, scroll="2m", size=size, body={"query": {"match_all": {}}})
	scrollId = response["_scroll_id"]
	hits = response["hits"]["hits"]
	totalIndexData = response['hits']['total']['value']
	print(f"Starting process with sequential processing scrollId={scrollId}")

	currentData = len(hits)
	totalData = currentData
	print(f"Writing to file [{filename}]")
	i=0
	while currentData > 0:
		for doc in hits:
			writetofile(filename, doc["_id"])
			i+=1
			print(f"\rProgress writing to file {i}/{totalIndexData}", end="")
		try:
			mctime = time.ctime()
			print(f"\r[{mctime}] Waiting for getting data... current position {i}/{totalIndexData}", end="\n")
			response = client.scroll(scroll_id=scrollId, scroll="2m")
			scrollId = response["_scroll_id"]
			hits = response["hits"]["hits"]
			currentData = len(hits)
			totalData+=currentData
		except Exception as e:
			print(f"Error {e}")
			os.remove(filename)
			client.clear_scroll(scroll_id=scrollId)
			print(f"Remove file and scroll id deleted")
			sys.exit(1)

	client.clear_scroll(scroll_id=scrollId)
	print(f"Successfully writing {totalData} data to file and scroll id deleted")

def writetofile(filename, data):	
	os.makedirs(os.path.dirname(filename), exist_ok=True)
	with open(filename, "a") as f:
		f.write(data + "\n")

def todelete(trnid):
    filename = '/hdd/actodel/acttodel/dumptodelete/contohdata'
    with open(filename, 'r') as f:
        data = f.read().splitlines()

    def splitdata(data, batch_size):
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    batches = list(splitdata(data, 40))
    
    poolSize = 6
    with Pool(poolSize) as pool:
        for batch in batches:
            if batch is not None:
                result = pool.map(readdata, batch)
                print(result)

def main(args):
	idx = args.index
	size = 100000 if args.size is None else args.size

	if idx is None:
		print("Abort for empty index")
		exit()

	filename = f"dumptodelete/{idx}.todelete"
	if os.path.exists(filename):
		print(f"Found file [{filename}] delete it manually and retry process")
		exit()

	getdata(idx, filename, size)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="rusdi kecap")
	parser.add_argument("-i", "--index", help="Name index for you want to retrieve data", type=str)
	parser.add_argument("-s", "--size", help="Size of max retrieve data", type=str)
	args = parser.parse_args()
	if len(sys.argv)==1:
		parser.print_help(sys.stderr)
		sys.exit(1)

	main(args)
