# -*- coding: utf-8 -*-
import csv
import tqdm

def title_parse(title):

    retval_0 = title.split(" ")[0]
    ret_name_0 = "community"

    return [retval_0], [ret_name_0]

def direction_parse(val):

    if val == "暂无数据":
        retval_0 = NULL
    
    else:
        retval_0 = "-".join(sorted(val.split(" ")))
    ret_name_0 = "orientation"

    return [retval_0], [ret_name_0]    

def floor_parse(val):

    if "(" not in val:
        retval_0, retval_1 = NULL, -1
    else:
        retval_0 = val.split("(")[0]
        try:
            retval_1 = int(val.split("层)")[0].split("(共")[-1])
        except:
            retval_0, retval_1 = NULL, -1
    ret_name_0 = "house_height"
    ret_name_1 = "building_height"
    return [retval_0, retval_1], [ret_name_0, ret_name_1]   

def total_price_parse(val):
    if "-" in val:
        try:
            uh, ul = val.split("-")
            retval_0 = int((uh + ul) / 2)
        except:
            retval_0 = -1
    else:
        try:
            retval_0 = int(val)
        except:
            retval_0 = -1
    ret_name_0 = "total_price"
    
    return [retval_0], [ret_name_0]    

def unit_price_parse(val):
    if "-" in val:
        try:
            uh, ul = val.split("-")
            retval_0 = int((int(uh) + int(ul)) / 2)
        except:
            retval_0 = -1
    else:
        try:
            retval_0 = int(val)
        except:
            retval_0 = -1
    ret_name_0 = "unit_price"
    
    return [retval_0], [ret_name_0]            

def room_structure_parse(val)

fl = [x for x in csv.reader(open("./sell.csv"))]
print(fl[3])
print(unit_price_parse('87562'))    
exit()
args = {"houseID":"p",
 "title": title_parse,
 "link":"p",
 "community":"p",
 "years":"n",
 "housetype":"n",
 "square":"n",
 "direction": direction_parse,
 "floor": floor_parse,
 "status":"n",
 "source":"n",
 "totalPrice": total_price_parse,
 "unitPrice": unit_price_parse,
 "detail_room_structure": room_structure_parse,
 "detail_floor":"n",
 "detail_building_area": building_area,
 "detail_flat":"p",
 "detail_usable_area": usable_area,
 "detail_building_type":"p",
 "detail_facing":"n",
 "detail_building_age":"p",
 "detail_decorate":"p",
 "detail_building_structure":"p",
 "detail_heating":"p",
 "detail_elevator2residents": elevator_parse,
 "detail_property_limit":"p",
 "detail_elevator":"p",
 "detail_lianjiaid":"p",
 "detail_market_identity":"p",
 "detail_first_online":"p",
 "detail_usage":"p",
 "detail_limit":"p",
 "detail_belonging":"p",
 "dealdate":"p",
 "updatedate":"p"
 }