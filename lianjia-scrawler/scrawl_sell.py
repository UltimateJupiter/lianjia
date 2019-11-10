import core
import sys
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

def get_last_community(city):
    res = None
    for i in model.Sellinfo.select():
        res = i.community
    if res is not None:
        print("last community: %s" % res)
        return res
    return

def get_communitylist_sorted(city):
    com = []
    for community in model.Community.select():
        if community.city == city:
            com.append([community.title, int(community.onsale)])
    com.sort(key = lambda x: -x[1])
    for i in range(10):
        print(com[i][0] + str(com[i][1]))
    ret = [x[0] for x in com]

    return ret


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
    model.database_init()
    
    communitylist = get_communitylist_sorted(city)
    if len(sys.argv) > 1:
        if sys.argv[1] == "C":
            break_point = get_last_community(city)
            print(break_point)
            tag = communitylist.index(break_point)
            communitylist = communitylist[tag:]
    core.GetSellByCommunitylist(city, communitylist)
