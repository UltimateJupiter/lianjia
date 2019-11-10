# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import model
import misc
import time
import datetime
import urllib2
import logging
from termcolor import colored

import multiprocessing
from multiprocessing import Queue, Process, Manager

translate_1 = {
u"房屋户型": u"detail_room_structure",
u"所在楼层": u"detail_floor",
u"建筑面积": u"detail_building_area",
u"户型结构": u"detail_flat",
u"套内面积": u"detail_usable_area",
u"建筑类型": u"detail_building_type",
u"房屋朝向": u"detail_facing",
u"建成年代": u"detail_building_age",
u"装修情况": u"detail_decorate",
u"建筑结构": u"detail_building_structure",
u"供暖方式": u"detail_heating",
u"梯户比例": u"detail_elevator2residents",
u"产权年限": u"detail_property_limit",
u"配备电梯": u"detail_elevator",
u"链家编号": u"detail_lianjiaid",
u"交易权属": u"detail_market_identity",
u"挂牌时间": u"detail_first_online",
u"房屋用途": u"detail_usage",
u"房屋年限": u"detail_limit",
u"房权所属": u"detail_belonging",
}

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

def get_district_of_city(city):
    ret = []
    logging.info("Get District Infomation")
    url = "https://%s.lianjia.com/chengjiao/" % (city)
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')
    if check_block(soup):
        return
    intro_wrapper = soup.find_all("div", {"data-role": "ershoufang"})[0]
    for x in intro_wrapper.find_all("a"):
        ret.append(x.get("href").split('/')[2])
    print(ret)
    return ret

def get_subregion_of_city(city):

    districts = get_district_of_city(city)
    ret = []
    for dis in districts:
        logging.info("Get Sub-District Infomation %s" % (dis))
        url = "https://%s.lianjia.com/xiaoqu/%s/" % (city, dis)
        source_code = misc.get_source_code(url)
        soup = BeautifulSoup(source_code, 'lxml')
        if check_block(soup):
            return
        intro_wrapper = soup.find_all("div", {"data-role": "ershoufang"})[0]
        for x in intro_wrapper.find_all("a"):
            name = x.get("href").split('/')[2]
            if name not in districts:
                ret.append(name)
        print(ret)
    return sorted(ret)

def GetHouseByCommunitylist(city, communitylist):
    logging.info("Get House Infomation")
    starttime = datetime.datetime.now()
    for community in communitylist:
        try:
            get_house_percommunity(city, community)
        except Exception as e:
            logging.error(e)
            logging.error(community + "Fail")
            pass
    endtime = datetime.datetime.now()
    logging.info("Run time: " + str(endtime - starttime))


def GetSellByCommunitylist(city, communitylist):
    logging.info("Get Sell Infomation")
    starttime = datetime.datetime.now()
    for community in communitylist:
        try:
            get_sell_percommunity(city, community)
        except Exception as e:
            logging.error(e)
            logging.error(community + "Fail")
            pass
    endtime = datetime.datetime.now()
    logging.info("Run time: " + str(endtime - starttime))


def GetRentByCommunitylist(city, communitylist):
    logging.info("Get Rent Infomation")
    starttime = datetime.datetime.now()
    for community in communitylist:
        try:
            get_rent_percommunity(city, community)
        except Exception as e:
            logging.error(e)
            logging.error(community + "Fail")
            pass
    endtime = datetime.datetime.now()
    logging.info("Run time: " + str(endtime - starttime))


def GetCommunityByRegionlist(city, regionlist=[u'xicheng']):
    logging.info("Get Community Infomation")
    starttime = datetime.datetime.now()
    for regionname in regionlist:
        try:
            get_community_perregion(city, regionname)
            logging.info(regionname + "Done")
        except Exception as e:
            logging.error(e)
            logging.error(regionname + "Fail")
            pass
    endtime = datetime.datetime.now()
    logging.info("Run time: " + str(endtime - starttime))


def GetHouseByRegionlist(city, regionlist=[u'xicheng']):
    starttime = datetime.datetime.now()
    for regionname in regionlist:
        logging.info("Get Onsale House Infomation in %s" % regionname)
        try:
            get_house_perregion(city, regionname)
        except Exception as e:
            logging.error(e)
            pass
    endtime = datetime.datetime.now()
    logging.info("Run time: " + str(endtime - starttime))


def GetRentByRegionlist(city, regionlist=[u'xicheng']):
    starttime = datetime.datetime.now()
    for regionname in regionlist:
        logging.info("Get Rent House Infomation in %s" % regionname)
        try:
            get_rent_perregion(city, regionname)
        except Exception as e:
            logging.error(e)
            pass
    endtime = datetime.datetime.now()
    logging.info("Run time: " + str(endtime - starttime))


def get_house_percommunity(city, communityname):
    baseUrl = u"http://%s.lianjia.com/" % (city)
    url = baseUrl + u"ershoufang/rs" + \
        urllib2.quote(communityname.encode('utf8')) + "/"
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')

    if check_block(soup):
        return
    total_pages = misc.get_total_pages(url)

    if total_pages == None:
        row = model.Houseinfo.select().count()
        raise RuntimeError("Finish at %s because total_pages is None" % row)

    for page in range(total_pages):
        if page > 0:
            url_page = baseUrl + \
                u"ershoufang/pg%drs%s/" % (page,
                                           urllib2.quote(communityname.encode('utf8')))
            source_code = misc.get_source_code(url_page)
            soup = BeautifulSoup(source_code, 'lxml')

        nameList = soup.findAll("li", {"class": "clear"})
        i = 0
        log_progress("GetHouseByCommunitylist - test",
                     communityname, page + 1, total_pages)
        data_source = []
        hisprice_data_source = []
        for name in nameList:  # per house loop
            i = i + 1
            info_dict = {}
            logging.info(name)
            try:
                housetitle = name.find("div", {"class": "title"})
                info_dict.update({u'title': housetitle.a.get_text().strip()})
                info_dict.update({u'link': housetitle.a.get('href')})

                houseaddr = name.find("div", {"class": "address"})
                info = houseaddr.div.get_text().split('|')
                info_dict.update({u'community': communityname})
                info_dict.update({u'housetype': info[1].strip()})
                info_dict.update({u'square': info[2].strip()})
                info_dict.update({u'direction': info[3].strip()})
                info_dict.update({u'decoration': info[4].strip()})

                housefloor = name.find("div", {"class": "flood"})
                floor_all = housefloor.div.get_text().split(
                    '-')[0].strip().split(' ')
                info_dict.update({u'floor': floor_all[0].strip()})
                info_dict.update({u'years': floor_all[-1].strip()})

                followInfo = name.find("div", {"class": "followInfo"})
                info_dict.update({u'followInfo': followInfo.get_text()})

                tax = name.find("div", {"class": "tag"})
                info_dict.update({u'taxtype': tax.get_text().strip()})

                totalPrice = name.find("div", {"class": "totalPrice"})
                info_dict.update({u'totalPrice': totalPrice.span.get_text()})

                unitPrice = name.find("div", {"class": "unitPrice"})
                info_dict.update({u'unitPrice': unitPrice.get('data-price')})
                info_dict.update({u'houseID': unitPrice.get('data-hid')})
            except:
                continue
            # houseinfo insert into mysql
            data_source.append(info_dict)
            hisprice_data_source.append(
                {"houseID": info_dict["houseID"], "totalPrice": info_dict["totalPrice"]})
            # model.Houseinfo.insert(**info_dict).upsert().execute()
            #model.Hisprice.insert(houseID=info_dict['houseID'], totalPrice=info_dict['totalPrice']).upsert().execute()

        with model.database.atomic():
            if data_source:
                model.Houseinfo.insert_many(data_source).upsert().execute()
            if hisprice_data_source:
                model.Hisprice.insert_many(
                    hisprice_data_source).upsert().execute()
        time.sleep(1)

def get_house_detail(url_page):

    ret = {}
    source_code = misc.get_source_code(url_page)
    soup = BeautifulSoup(source_code, 'lxml')
    if check_block(soup):
        return
    intro_wrapper = soup.find(id='introduction')
    for ultag in intro_wrapper.findAll('ul'):
        for litag in ultag.find_all('li'):
            key = litag.find_all('span')[0]
            ret_key = translate_1[key.text]
            # print(ret_key, ret_key in translate_1)
            ret[ret_key] = litag.text[len(key.text):].strip().encode("utf-8")
    # print(ret)
    return ret

def get_sell_worker(nameid_q, names, share_ls, communityname, city):

    while True:
        try:
            if nameid_q.qsize() == 0:
                return
            name_id = nameid_q.get_nowait()
        except:
            if nameid_q.qsize() == 0:
                return
            else:
                continue
        name = names[name_id]
        info_dict = {}
        try:
            housetitle = name.find("div", {"class": "title"})
            link_detail = housetitle.a.get('href')
            info_dict.update({u'title': housetitle.get_text().strip()})
            info_dict.update({u'link': housetitle.a.get('href')})
            houseID = housetitle.a.get(
                'href').split("/")[-1].split(".")[0]
            info_dict.update({u'houseID': houseID.strip()})

            house = housetitle.get_text().strip().split(' ')
            info_dict.update({u'community': communityname})
            info_dict.update(
                {u'housetype': house[1].strip() if 1 < len(house) else ''})
            info_dict.update(
                {u'square': house[2].strip() if 2 < len(house) else ''})

            houseinfo = name.find("div", {"class": "houseInfo"})
            info = houseinfo.get_text().split('|')
            info_dict.update({u'direction': info[0].strip()})
            info_dict.update(
                {u'status': info[1].strip() if 1 < len(info) else ''})

            housefloor = name.find("div", {"class": "positionInfo"})
            floor_all = housefloor.get_text().strip().split(' ')
            info_dict.update({u'floor': floor_all[0].strip()})
            info_dict.update({u'years': floor_all[-1].strip()})

            followInfo = name.find("div", {"class": "source"})
            info_dict.update(
                {u'source': followInfo.get_text().strip()})

            totalPrice = name.find("div", {"class": "totalPrice"})
            if totalPrice.span is None:
                info_dict.update(
                    {u'totalPrice': totalPrice.get_text().strip()})
            else:
                info_dict.update(
                    {u'totalPrice': totalPrice.span.get_text().strip()})

            unitPrice = name.find("div", {"class": "unitPrice"})
            if unitPrice.span is None:
                info_dict.update(
                    {u'unitPrice': unitPrice.get_text().strip()})
            else:
                info_dict.update(
                    {u'unitPrice': unitPrice.span.get_text().strip()})

            dealDate = name.find("div", {"class": "dealDate"})
            info_dict.update(
                {u'dealdate': dealDate.get_text().strip().replace('.', '-')})

            details = get_house_detail(link_detail)
            if details is not None:
                info_dict.update(details)

        except:
            logging.info("Fail")
            exit()
            continue

        share_ls.append(info_dict)

def get_sell_percommunity(city, communityname, threads=30):
    baseUrl = u"http://%s.lianjia.com/" % (city)
    url = baseUrl + u"chengjiao/rs" + \
        urllib2.quote(communityname.encode('utf8')) + "/"
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')

    if check_block(soup):
        return
    total_pages = misc.get_total_pages(url)

    if total_pages == None:
        row = model.Sellinfo.select().count()
        raise RuntimeError("Finish at %s because total_pages is None" % row)

    for page in range(total_pages):
        if page > 0:
            url_page = baseUrl + \
                u"chengjiao/pg%drs%s/" % (page,
                                          urllib2.quote(communityname.encode('utf8')))
            source_code = misc.get_source_code(url_page)
            soup = BeautifulSoup(source_code, 'lxml')
        log_progress("GetSellByCommunitylist",
                     communityname, page + 1, total_pages)
        # logging.info("start")

        data_source = []
        
        nameList = []
        for ultag in soup.findAll("ul", {"class": "listContent"}):
            for name in ultag.find_all('li'):
                nameList.append(name)

        info_ls_mult = Manager().list()
        nameid_q = Queue()
        for i in range(len(nameList)):
            nameid_q.put(i)
        
        processes = []
        try:
            for i in range(threads):
                proc = Process(target=get_sell_worker, args=(nameid_q, nameList, info_ls_mult, communityname, city,))
                processes.append(proc)
                proc.start()
            
            for proc in processes:
                proc.join()

        except KeyboardInterrupt:
            print("Emergency terminate")
            print("killing %d processes" % (len(processes)))
            for proc in processes:
                proc.terminate()
            
        data_source = list(info_ls_mult)
        if len(data_source) == 0:
            print(colored("sth is wrong with %s, give up on this one" % communityname, "red"))
            break
        print("Finished with %d at %s" % (len(data_source), communityname))

        with model.database.atomic():
            if data_source:
                model.Sellinfo.insert_many(data_source).upsert().execute()
                logging.info("Writing to database")
        time.sleep(1)

def community_info_worker(nameid_q, names, share_ls, regionname, city):

    while True:
        try:
            if nameid_q.qsize() == 0:
                return
            name_id = nameid_q.get_nowait()
        except:
            if nameid_q.qsize() == 0:
                return
            else:
                continue
        name = names[name_id]
        info_dict = {}
        communitytitle = name.find("div", {"class": "title"})
        title = communitytitle.get_text().strip('\n')

        #logging.info("Analyzing: %s - %s" % (title, regionname))
        link = communitytitle.a.get('href')
        info_dict.update({u'title': title})
        info_dict.update({u'link': link})

        district = name.find("a", {"class": "district"})
        info_dict.update({u'district': district.get_text()})

        bizcircle = name.find("a", {"class": "bizcircle"})
        info_dict.update({u'bizcircle': bizcircle.get_text()})

        tagList = name.find("div", {"class": "tagList"})
        info_dict.update({u'tagList': tagList.get_text().strip('\n')})

        onsale = name.find("a", {"class": "totalSellCount"})
        info_dict.update(
            {u'onsale': onsale.span.get_text().strip('\n')})

        onrent = name.find("a", {"title": title + u"租房"})
        info_dict.update(
            {u'onrent': onrent.get_text().strip('\n').split(u'套')[0]})

        info_dict.update({u'id': name.get('data-housecode')})

        price = name.find("div", {"class": "totalPrice"})
        info_dict.update({u'price': price.span.get_text().strip('\n')})

        communityinfo = get_communityinfo_by_url(link)
        for key, value in communityinfo.iteritems():
            info_dict.update({key: value})

        info_dict.update({u'city': city})

        share_ls.append(info_dict)


def get_community_perregion(city, regionname=u'xicheng', threads=30):
    baseUrl = u"http://%s.lianjia.com/" % (city)
    url = baseUrl + u"xiaoqu/" + regionname + "/"
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')

    if check_block(soup):
        return
    total_pages = misc.get_total_pages(url)

    if total_pages == None:
        row = model.Community.select().count()
        raise RuntimeError("Finish at %s because total_pages is None" % row)

    for page in range(total_pages):
        try:
            if page > 0:
                url_page = baseUrl + u"xiaoqu/" + regionname + "/pg%d/" % page
                source_code = misc.get_source_code(url_page)
                soup = BeautifulSoup(source_code, 'lxml')

            nameList = soup.findAll("li", {"class": "clear"})
            i = 0
            log_progress("GetCommunityByRegionlist",
                        regionname, page + 1, total_pages)
            data_source = []
            # for name in nameList[:1]:  # Per house loop DEBUGGING
            info_ls_mult = Manager().list()
            nameid_q = Queue()
            for i in range(len(nameList)):
                nameid_q.put(i)
            
            processes = []
            try:
                for i in range(threads):
                    proc = Process(target=community_info_worker, args=(nameid_q, nameList, info_ls_mult, regionname, city,))
                    processes.append(proc)
                    proc.start()
                
                for proc in processes:
                    proc.join()

            except KeyboardInterrupt:
                print("Emergency terminate")
                print("killing %d processes" % (len(processes)))
                for proc in processes:
                    proc.terminate()
                
            data_source = list(info_ls_mult)
            if len(data_source) == 0:
                print(colored("sth is wrong with %s, give up on this one" % regionname, "red"))
                break
            print("Finished with %d at %s" % (len(data_source), regionname))
            
            with model.database.atomic():
                print("submitting to dataset")
                if data_source:
                    model.Community.insert_many(data_source).upsert().execute()
                if page % 4 == 0:
                    cnt = []
                    for community in model.Community.select():
                        if community.city == city:
                            cnt.append(community.title)
                    print(" %d Community scraped: %d" % (page, len(cnt)))
            time.sleep(2)
        except:
            print(colored("Failed at %d - %s" % (page, regionname), "red"))
            continue


def get_rent_percommunity(city, communityname):
    baseUrl = u"http://%s.lianjia.com/" % (city)
    url = baseUrl + u"zufang/rs" + \
        urllib2.quote(communityname.encode('utf8')) + "/"
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')

    if check_block(soup):
        return
    total_pages = misc.get_total_pages(url)

    if total_pages == None:
        row = model.Rentinfo.select().count()
        raise RuntimeError("Finish at %s because total_pages is None" % row)

    for page in range(total_pages):
        if page > 0:
            url_page = baseUrl + \
                u"rent/pg%drs%s/" % (page,
                                     urllib2.quote(communityname.encode('utf8')))
            source_code = misc.get_source_code(url_page)
            soup = BeautifulSoup(source_code, 'lxml')
        i = 0
        log_progress("GetRentByCommunitylist",
                     communityname, page + 1, total_pages)
        data_source = []
        for ultag in soup.findAll("ul", {"class": "house-lst"}):
            for name in ultag.find_all('li'):
                i = i + 1
                info_dict = {}
                try:
                    housetitle = name.find("div", {"class": "info-panel"})
                    info_dict.update({u'title': housetitle.get_text().strip()})
                    info_dict.update({u'link': housetitle.a.get('href')})
                    houseID = housetitle.a.get(
                        'href').split("/")[-1].split(".")[0]
                    info_dict.update({u'houseID': houseID})

                    region = name.find("span", {"class": "region"})
                    info_dict.update({u'region': region.get_text().strip()})

                    zone = name.find("span", {"class": "zone"})
                    info_dict.update({u'zone': zone.get_text().strip()})

                    meters = name.find("span", {"class": "meters"})
                    info_dict.update({u'meters': meters.get_text().strip()})

                    other = name.find("div", {"class": "con"})
                    info_dict.update({u'other': other.get_text().strip()})

                    subway = name.find("span", {"class": "fang-subway-ex"})
                    if subway is None:
                        info_dict.update({u'subway': ""})
                    else:
                        info_dict.update(
                            {u'subway': subway.span.get_text().strip()})

                    decoration = name.find("span", {"class": "decoration-ex"})
                    if decoration is None:
                        info_dict.update({u'decoration': ""})
                    else:
                        info_dict.update(
                            {u'decoration': decoration.span.get_text().strip()})

                    heating = name.find("span", {"class": "heating-ex"})
                    info_dict.update(
                        {u'heating': heating.span.get_text().strip()})

                    price = name.find("div", {"class": "price"})
                    info_dict.update(
                        {u'price': int(price.span.get_text().strip())})

                    pricepre = name.find("div", {"class": "price-pre"})
                    info_dict.update(
                        {u'pricepre': pricepre.get_text().strip()})

                except:
                    continue
                # Rentinfo insert into mysql
                data_source.append(info_dict)
                # model.Rentinfo.insert(**info_dict).upsert().execute()

        with model.database.atomic():
            if data_source:
                model.Rentinfo.insert_many(data_source).upsert().execute()
        time.sleep(1)


def get_house_perregion(city, district):
    baseUrl = u"http://%s.lianjia.com/" % (city)
    url = baseUrl + u"ershoufang/%s/" % district
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')
    if check_block(soup):
        return
    total_pages = misc.get_total_pages(url)
    if total_pages == None:
        row = model.Houseinfo.select().count()
        raise RuntimeError("Finish at %s because total_pages is None" % row)

    for page in range(total_pages):
        if page > 0:
            url_page = baseUrl + u"ershoufang/%s/pg%d/" % (district, page)
            source_code = misc.get_source_code(url_page)
            soup = BeautifulSoup(source_code, 'lxml')
        i = 0
        log_progress("GetHouseByRegionlist", district, page + 1, total_pages)
        data_source = []
        hisprice_data_source = []
        for ultag in soup.findAll("ul", {"class": "sellListContent"}):
            for name in ultag.find_all('li'):
                i = i + 1
                info_dict = {}
                try:
                    housetitle = name.find("div", {"class": "title"})
                    info_dict.update(
                        {u'title': housetitle.a.get_text().strip()})
                    info_dict.update({u'link': housetitle.a.get('href')})
                    houseID = housetitle.a.get('data-housecode')
                    info_dict.update({u'houseID': houseID})

                    houseinfo = name.find("div", {"class": "houseInfo"})
                    info = houseinfo.get_text().split('|')
                    #info_dict.update({u'community': info[0]})
                    info_dict.update({u'housetype': info[0]})
                    info_dict.update({u'square': info[1]})
                    info_dict.update({u'direction': info[2]})
                    info_dict.update({u'decoration': info[3]})
                    info_dict.update({u'floor': info[4]})
                    info_dict.update({u'years': info[5]})

                    housefloor = name.find("div", {"class": "positionInfo"})
                    communityInfo = housefloor.get_text().split('-')
                    info_dict.update({u'community': communityInfo[0]})
                    #info_dict.update({u'years': housefloor.get_text().strip()})
                    #info_dict.update({u'floor': housefloor.get_text().strip()})

                    followInfo = name.find("div", {"class": "followInfo"})
                    info_dict.update(
                        {u'followInfo': followInfo.get_text().strip()})

                    taxfree = name.find("span", {"class": "taxfree"})
                    if taxfree == None:
                        info_dict.update({u"taxtype": ""})
                    else:
                        info_dict.update(
                            {u"taxtype": taxfree.get_text().strip()})

                    totalPrice = name.find("div", {"class": "totalPrice"})
                    info_dict.update(
                        {u'totalPrice': totalPrice.span.get_text()})

                    unitPrice = name.find("div", {"class": "unitPrice"})
                    info_dict.update(
                        {u'unitPrice': unitPrice.get("data-price")})
                except:
                    continue

                # Houseinfo insert into mysql
                data_source.append(info_dict)
                hisprice_data_source.append(
                    {"houseID": info_dict["houseID"], "totalPrice": info_dict["totalPrice"]})
                # model.Houseinfo.insert(**info_dict).upsert().execute()
                #model.Hisprice.insert(houseID=info_dict['houseID'], totalPrice=info_dict['totalPrice']).upsert().execute()

        with model.database.atomic():
            if data_source:
                model.Houseinfo.insert_many(data_source).upsert().execute()
            if hisprice_data_source:
                model.Hisprice.insert_many(
                    hisprice_data_source).upsert().execute()
        time.sleep(1)


def get_rent_perregion(city, district):
    baseUrl = u"http://%s.lianjia.com/" % (city)
    url = baseUrl + u"zufang/%s/" % district
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')
    if check_block(soup):
        return
    total_pages = misc.get_total_pages(url)
    if total_pages == None:
        row = model.Rentinfo.select().count()
        raise RuntimeError("Finish at %s because total_pages is None" % row)

    for page in range(total_pages):
        if page > 0:
            url_page = baseUrl + u"zufang/%s/pg%d/" % (district, page)
            source_code = misc.get_source_code(url_page)
            soup = BeautifulSoup(source_code, 'lxml')
        i = 0
        log_progress("GetRentByRegionlist", district, page + 1, total_pages)
        data_source = []
        for ultag in soup.findAll("ul", {"class": "house-lst"}):
            for name in ultag.find_all('li'):
                i = i + 1
                info_dict = {}
                try:
                    housetitle = name.find("div", {"class": "info-panel"})
                    info_dict.update(
                        {u'title': housetitle.h2.a.get_text().strip()})
                    info_dict.update({u'link': housetitle.a.get("href")})
                    houseID = name.get("data-housecode")
                    info_dict.update({u'houseID': houseID})

                    region = name.find("span", {"class": "region"})
                    info_dict.update({u'region': region.get_text().strip()})

                    zone = name.find("span", {"class": "zone"})
                    info_dict.update({u'zone': zone.get_text().strip()})

                    meters = name.find("span", {"class": "meters"})
                    info_dict.update({u'meters': meters.get_text().strip()})

                    other = name.find("div", {"class": "con"})
                    info_dict.update({u'other': other.get_text().strip()})

                    subway = name.find("span", {"class": "fang-subway-ex"})
                    if subway == None:
                        info_dict.update({u'subway': ""})
                    else:
                        info_dict.update(
                            {u'subway': subway.span.get_text().strip()})

                    decoration = name.find("span", {"class": "decoration-ex"})
                    if decoration == None:
                        info_dict.update({u'decoration': ""})
                    else:
                        info_dict.update(
                            {u'decoration': decoration.span.get_text().strip()})

                    heating = name.find("span", {"class": "heating-ex"})
                    if decoration == None:
                        info_dict.update({u'heating': ""})
                    else:
                        info_dict.update(
                            {u'heating': heating.span.get_text().strip()})

                    price = name.find("div", {"class": "price"})
                    info_dict.update(
                        {u'price': int(price.span.get_text().strip())})

                    pricepre = name.find("div", {"class": "price-pre"})
                    info_dict.update(
                        {u'pricepre': pricepre.get_text().strip()})

                except:
                    continue
                # Rentinfo insert into mysql
                data_source.append(info_dict)
                # model.Rentinfo.insert(**info_dict).upsert().execute()

        with model.database.atomic():
            if data_source:
                model.Rentinfo.insert_many(data_source).upsert().execute()
        time.sleep(1)


def get_communityinfo_by_url(url):
    source_code = misc.get_source_code(url)
    soup = BeautifulSoup(source_code, 'lxml')

    if check_block(soup):
        return

    communityinfos = soup.findAll("div", {"class": "xiaoquInfoItem"})
    res = {}
    for info in communityinfos:
        key_type = {
            u"建筑年代": u'year',
            u"建筑类型": u'housetype',
            u"物业费用": u'cost',
            u"物业公司": u'service',
            u"开发商": u'company',
            u"楼栋总数": u'building_num',
            u"房屋总数": u'house_num',
        }
        try:
            key = info.find("span", {"xiaoquInfoLabel"})
            value = info.find("span", {"xiaoquInfoContent"})
            key_info = key_type[key.get_text().strip()]
            value_info = value.get_text().strip()
            res.update({key_info: value_info})

        except:
            continue
    return res


def check_block(soup):
    if soup.title.string == "414 Request-URI Too Large":
        logging.error(
            "Lianjia block your ip, please verify captcha manually at lianjia.com")
        return True
    return False


def log_progress(function, address, page, total):
    logging.info("Progress: %s %s: current page %d total pages %d" %
                 (function, address, page, total))
