import core
import model
import time
import settings
import logging
import multiprocessing
from multiprocessing import Queue, Process

def get_communitylist(city):
    res = []
    for community in model.Community.select():
        if community.city == city:
            res.append(community.title)
    return res

def get_community_by_regions(regions, city, threads):

    regions_q = Queue()
    for r in regions:
        regions_q.put(r)

    processes = []
    try:
        for i in range(threads):
            proc = Process(target=get_community_worker, args=(regions_q, city,))
            processes.append(proc)
            time.sleep(4)
            proc.start()
        
        for proc in processes:
            proc.join()

    except KeyboardInterrupt:
        print("Emergency terminate")
        print("killing %d processes" % (len(processes)))
        for proc in processes:
            proc.terminate()
        
    print("\n\n====\nFinished\n====\n\n")

    
def get_community_worker(queue, city):
    while True:
        try:
            reg = queue.get_nowait()
            print(reg)
        except:
            return
        logging.info("Processing" + reg + "" + city)
        core.GetCommunityByRegionlist(city, [reg])

if __name__ == "__main__":
    city = settings.CITY
    regionlist = core.get_district_of_city(city)
    model.database_init()
    
    communitylist = get_communitylist(city)
    core.GetSellByCommunitylist(city, communitylist)
