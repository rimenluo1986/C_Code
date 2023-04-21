# -*- coding:utf-8 -*-

import requests
import json
import re
import difflib
import time
import threading
import pandas as pd
from pymongo import MongoClient


REPLICASET_URL = 'mongodb://db_test:H10]cc2#yJ8zTvrR@dds-wz9b5ff04c3356e41182-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9b5ff04c3356e42730-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9b5ff04c3356e43124-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9b5ff04c3356e44962-pub.mongodb.rds.aliyuncs.com:3717/test?replicaSet=mgset-66293787'
client = MongoClient(REPLICASET_URL)

sess = requests.session()
threading_num = threading.Semaphore(15)                 # 线程数量


def crawl_run():
    # hotel_files = pd.read_excel(r'D:\DEMO\agoda\save\agoda_cn.xlsx')
    hotel_files = client.test.agoda_hotels.find()
    hotel_files_list = []
    for i in hotel_files:
        hotel_files_list.append(i)
    url = 'https://www.agoda.cn/zh-cn/noa/hotel/shenzhen-cn.html'
    # correlationId, sessionId = parse_correlationId_sessionId(url)  # 解析获取correlationId, sessionId
    for i in range(0, len(hotel_files_list)):

        hotel_code = int(hotel_files_list[i]['hotel_code'])
        name_en = hotel_files_list[i]['name_en']
        name_cn = hotel_files_list[i]['name_cn']

        yes_no = mongodb_find(hotel_code)                           # 查找库是否有数据

        if len(yes_no) == 0:
            ts = threading.Thread(target=run, args=(hotel_code, name_en, name_cn, i, hotel_files_list, url))
            ts.start()


def mongodb_find(hotel_code):
    '''
    查找库是否有数据
    :return: yes_no
    '''
    find_id = client.test.agoda_test.find({'id': hotel_code})
    yes_no = [yes for yes in find_id]
    return yes_no


def run(hotel_code, name_en, name_cn, num, hotel_files, url):
    with threading_num:
        print('总数{}, 第{}家 : {}({})_{}'.format(len(hotel_files), num, name_cn, name_en, hotel_code))

        # checkIn = datetime.date.today()
        all_dict = {}
        link = 'https://www.agoda.cn/api/cronos/property/BelowFoldParams/GetSecondaryData?hotel_id={}&all=false&price_view=2&sessionid=-6164117942132671267&pagetypeid=7'.format(hotel_code)
        req2 = sess.get(link, headers=headers(url))
        time.sleep(5)
        req2_json = json.loads(req2.text)
        hotel_id = req2_json['hotelId']  # 酒店Id
        map_type = req2_json['mapParams']['mapProviderName']  # 地图类型
        hotel_name_cn = req2_json['aboutHotel']['translatedHotelName']  # 酒店名称
        hotel_name_en = req2_json['aboutHotel']['hotelName']  # 酒店名称
        all_dict['id'] = hotel_code
        all_dict['agoda_id'] = hotel_id
        all_dict['name_en'] = hotel_name_en
        all_dict['name_cn'] = hotel_name_cn
        all_dict['map_type'] = map_type
        all_dict['hotels'] = []
        all_dict['rooms'] = []

        pa = judge(name_en, name_cn, hotel_name_en, hotel_name_cn)

        if pa == 'yes':
            try:
                get_about_hotels(all_dict, req2_json)  # 解析获取关于酒店信息
                get_about_rooms(all_dict, req2_json)  # 解析获取关于房间信息
                print('完成...')
                client.test.agoda_test.insert(all_dict)
            except KeyError as e:
                client.test.agoda_test.insert(all_dict)
                print('{}:{},{},{},{},{}'.format(e, name_en, name_cn, hotel_name_en, hotel_name_cn, hotel_code))
        else:
            print('名称不一致')
            all_dict['name_en'] = name_en
            all_dict['name_cn'] = name_cn
            client.test.agoda_test.insert(all_dict)
            print('表：{}，网：{}'.format(name_en, hotel_name_en))
            print('表：{}，网：{}'.format(name_cn, hotel_name_cn))
        print('\n')


def judge(name_en, name_cn, hotel_name_en, hotel_name_cn):
    try:
        aa = get_equal_rate_1(name_en, hotel_name_en)
    except:
        aa = 0
    try:
        bb = get_equal_rate_1(name_cn, hotel_name_cn)
    except:
        bb = 0
    if aa == 1 or bb == 1:
        return 'yes'
    else:
        return 'no'


def get_about_hotels(all_dict, json_files):
    '''
    解析获取关于酒店信息
    :param json_files:
    :return: dict
    '''

    try:
        hotel_message = json_files['aboutHotel']['hotelDesc']['overview']                               # 酒店详情
    except:
        hotel_message = ''
    try:
        hotel_image = 'https:' + json_files['inquiryProperty']['hotelImage']                            # 酒店图片
    except:
        hotel_image = []

    hotel_feature = json_files['aboutHotel']['featureGroups']                                           # 设施服务
    feature_groups = parse_hotel_feature(hotel_feature)

    breakfast = json_files['breakfastInformation']                                                      # 早餐选项
    hotel_restaurant = json_files['restaurantOnSite']                                                   # 餐饮
    restaurant = parse_hotel_restaurant(hotel_restaurant, breakfast)

    try:
        hotel_near = json_files['essentialInfo']['nearbyProperties']                                    # 位置评分
    except KeyError:
        hotel_near = []
    position = parse_hotel_near(hotel_near)

    hotel_ambitus = json_files['aboutHotel']['placesOfInterest']['placesOfInterestProperties']          # 酒店周边
    periphery = parse_hotel_ambitus(hotel_ambitus)
    try:
        hotel_policies = json_files['aboutHotel']['guestPolicies']                                      # 住宿规定
    except:
        hotel_policies = ''
    try:
        hotel_other_policies = json_files['aboutHotel']['otherPolicies']                                # 住宿规定
    except:
        hotel_other_policies = ''

    policies = parse_hotel_policies(hotel_policies, hotel_other_policies)

    hotel_info = json_files['aboutHotel']['usefulInfoGroups']                                           # 实用信息
    information = parse_hotel_info(hotel_info)

    hotel_images = json_files['mosaicInitData']['images']                                               # 图片
    images = parse_hotel_images(hotel_images)

    hotel_addres = json_files['hotelInfo']['address']                                                   # 地址
    hotel_lat_lng = json_files['mapParams']['latlng']                                                   # 经纬度
    addres = parse_hotel_addres(hotel_addres, hotel_lat_lng)

    all_dict['hotels'] = {'features': feature_groups, 'restaurant': restaurant, 'position': position, 'periphery': periphery, 'policies': policies, 'information': information, 'images': images, 'addres': addres, 'hotel_image': hotel_image, 'hotel_message': hotel_message}


def get_about_rooms(all_dict, json_files):
    '''
    解析获取关于房间信息
    :param json_files:
    :return: all_list
    '''

    all_list = []
    for i in json_files['datelessMasterRoomInfo']:
        aa_dict = {}
        room_name = i['name']                                           # 房间名称
        room_id = i['roomid']                                           # 房间id

        try:
            beds = i['bedConfigurationSummary']['title']                # 床型
        except:
            beds = ''

        facility_groups = i['facilityGroups']                           # 房间配套
        room_set = parse_room_facility_groups(facility_groups)

        features_dict = [i['title'] for i in i['features']]             # 特色

        images = i['images']                                            # 房间图片
        images_dict = parse_room_images(images)

        aa_dict['name'] = room_name
        aa_dict['id'] = room_id
        aa_dict['beds'] = beds
        aa_dict['features'] = features_dict
        aa_dict['facilities'] = room_set
        aa_dict['images'] = images_dict

        all_list.append(aa_dict)
    all_dict['rooms'] = all_list


def parse_room_images(images):
    '''
    解析房间图片数据结构
    :param images:
    :return:
    '''
    images_list = ['https:' + i for i in images]
    return images_list


def parse_features(features):
    '''
    解析房间特色数据结构
    :param features:
    :return:
    '''
    features_list = [i['title'] for i in features]
    return features_list


def parse_room_facility_groups(facility_groups):
    '''
    解析房间配套数据结构
    :param facility_groups:
    :return:
    '''
    facility_groups_list = []
    for k in facility_groups:
        name = k['name']
        facility_groups_dict = {}
        facilities_list = [i['title'] for i in k['facilities']]
        facility_groups_dict['name'] = name
        facility_groups_dict['groups'] = facilities_list
        facility_groups_list.append(facility_groups_dict)
    return facility_groups_list


def parse_hotel_images(hotel_images):
    '''
    解析酒店图片数据结构
    :param hotel_images:
    :return:
    '''
    aa_list = []
    images_group_list = []
    for images in hotel_images:
        group = images['group']
        images_group_list.append(group)
    images_group_list = list(set(images_group_list))

    for i in images_group_list:
        images_list = []
        images_dict = {}
        for images in hotel_images:
            group = images['group']
            if i == group:
                images_url = 'https:' + images['location']
                images_list.append(images_url)
        images_dict['name'] = i
        images_dict['groups'] = images_list
        aa_list.append(images_dict)
    return aa_list


def parse_hotel_policies(hotel_policies, hotel_other_policies):
    '''
    解析住宿规定数据结构
    :param hotel_policies:
    :return:
    '''
    hotel_policies['other_policies'] = hotel_other_policies

    return hotel_policies


def parse_hotel_restaurant(hotel_restaurant, breakfast):
    '''
    解析餐厅数据结构
    :param hotel_restaurant:
    :return:
    '''
    aa_dict = {}
    restaurant_list = []

    if len(hotel_restaurant) > 0:
        for res in hotel_restaurant:
            try:
                del res['id']
            except:
                pass
            try:
                del res['name']
            except:
                pass
            restaurant_list.append(res)
    try:
        aa_dict['breakfast'] = breakfast['cuisines']
    except KeyError:
        aa_dict['breakfast'] = []

    restaurant_list.append(aa_dict)
    return restaurant_list


def parse_hotel_addres(hotel_addres, hotel_lat_lng):
    '''
    解析酒店地址数据结构
    :return:
    '''
    addres_dict = {}
    addres = hotel_addres['address']
    area_name = hotel_addres['areaName']
    city_name = hotel_addres['cityName']
    country_name = hotel_addres['countryName']
    postal_code = hotel_addres['postalCode']
    addres_full = hotel_addres['full']
    addres_dict['addres'] = addres
    addres_dict['area_name'] = area_name
    addres_dict['city_name'] = city_name
    addres_dict['country_name'] = country_name
    addres_dict['postal_code'] = postal_code
    addres_dict['addres_full'] = addres_full
    addres_dict['lat'] = hotel_lat_lng[0]
    addres_dict['lng'] = hotel_lat_lng[1]
    return addres_dict


def parse_hotel_near(hotel_near):
    '''
    解析位置评分数据结构
    :return:
    '''
    hotel_near_list = []
    for near in hotel_near:
        near_dict_1 = {}
        near_items = []
        name = near['categoryName']
        for places in near['places']:
            places_dict_2 = {}
            title = places['name']
            distance = places['distanceDisplayShort']
            latitude = places['latitude']
            longitude = places['longitude']
            places_dict_2['name'] = title
            places_dict_2['distance'] = distance
            places_dict_2['lat'] = latitude
            places_dict_2['lng'] = longitude
            near_items.append(places_dict_2)
        near_dict_1['name'] = name
        near_dict_1['groups'] = near_items
        hotel_near_list.append(near_dict_1)
    return hotel_near_list


def parse_hotel_info(hotel_info):
    '''
    解析实用信息数据结构
    :return:
    '''
    hotel_info_list = []
    for info in hotel_info:
        info_dict_1 = {}
        info_items = []
        name = info['name']
        for items in info['items']:
            info_dict_2 = {}
            title = items['title']
            try:
                description = items['description']
            except:
                description = ''
            info_dict_2['title'] = title
            info_dict_2['description'] = description
            info_items.append(info_dict_2)
        info_dict_1['name'] = name
        info_dict_1['groups'] = info_items
        hotel_info_list.append(info_dict_1)
    return hotel_info_list


def parse_hotel_ambitus(hotel_ambitus):
    '''
    解析酒店周边数据结构
    :param hotel_ambitus:
    :return:
    '''
    palces = []
    for ambitus in hotel_ambitus:
        category_name = ambitus['categoryName']
        places_dict = {}
        places_list = []

        places_dict['name'] = category_name
        places_dict['groups'] = places_list

        for places in ambitus['places']:
            places_dict_2 = {}
            name = places['name']
            distance = places['distanceDisplay']
            landmarkTypeName = places['landmarkTypeName']
            try:
                latitude = places['latitude']
                longitude = places['longitude']
            except:
                latitude = ''
                longitude = ''
            places_dict_2['name'] = name
            places_dict_2['distance'] = distance
            places_dict_2['type'] = landmarkTypeName
            places_dict_2['lat'] = latitude
            places_dict_2['lng'] = longitude
            places_list.append(places_dict_2)
        palces.append(places_dict)
    return palces


def parse_hotel_feature(hotel_feature):
    '''
    解析设施服务数据结构
    :param hotel_feature:
    :return:
    '''
    feature_list = []
    for feature in hotel_feature:
        feature_dict = {}
        feature_list_2 = [f['name'] for f in feature['feature']]
        feature_dict['name'] = feature['name']
        feature_dict['feature'] = feature_list_2
        feature_list.append(feature_dict)

    return feature_list


def parse_correlationId_sessionId(url):
    '''
    解析获取 correlationId, sessionId
    :param url:
    :return: correlationId, sessionId
    '''
    req = sess.get(url, headers=get_id_headers(), allow_redirects=False)
    correlationId = re.findall('{"correlationId":"(.*?)",', req.text)[0]
    sessionId = re.findall('"analyticsSessionId":"(.*?)"', req.text)[0]
    # searchrequestid = re.findall('searchrequestid=(.*?)&', req.text)[0]
    return correlationId, sessionId


# 相似度判断
def get_equal_rate_1(str1, str2):
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()


def headers(referer):
    headers = {
        'Host': 'www.agoda.cn',
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'AG-Language-Id': '8',
        'CR-Currency-Id': '15',
        'AG-Analytics-Session-Id': '-6164117942132671267',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-raw-url': '/zh-cn/noa/hotel/shenzhen-cn.html',
        'Content-type': 'application/json;charset=UTF-8',
        'AG-Language-Locale': 'zh-cn',
        'X-Requested-With': 'XMLHttpRequest',
        'x-referer': '',
        'AG-Correlation-Id': '061374c2-2917-4a1f-bdc1-b75b0b994a2d',
        'CR-Currency-Code': 'CNY',
        'sec-ch-ua-platform': '"Windows"',
        'Accept': '*/*',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': referer,
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        # 'Cookie': 'agoda.user.03=UserId=595ba893-8bde-4793-8fb2-c9890227500a; agoda.prius=PriusID=0&PointsMaxTraffic=Agoda; agoda.price.01=PriceView=2; agoda.consent=CN||2023-04-13 10:43:35Z; agoda.familyMode=Mode=0; _ab50group=GroupB; _40-40-20Split=Group40B; __gads=ID=e1e93c9eb505cddb:T=1681357461:S=ALNI_MZpUEaxxnr7xp3VE1_MYAkoAo9nFg; agoda.vuser=UserId=e5a4065e-9af1-4121-8c40-e3835a570242; deviceId=b14fbe39-b781-4397-b06b-dad27b9a9580; _ga=GA1.2.674414891.1681367313; _gid=GA1.2.1606365562.1681367313; agoda.firstclicks=-1||||2023-04-14T07:51:27||e30fdizkfprom1cfbrdvwwdk||{"IsPaid":false,"gclid":"","Type":""}; agoda.lastclicks=-1||||2023-04-14T07:51:27||e30fdizkfprom1cfbrdvwwdk||{"IsPaid":false,"gclid":"","Type":""}; agoda.landings=-1|||e30fdizkfprom1cfbrdvwwdk|2023-04-14T07:51:27|False|19-----1|||e30fdizkfprom1cfbrdvwwdk|2023-04-14T07:51:27|False|20-----1|||e30fdizkfprom1cfbrdvwwdk|2023-04-14T07:51:27|False|99; agoda.attr.03=ATItems=-1$04-14-2023 07:51$; agoda.version.03=CookieId=b054eb7e-1b41-47da-bb24-24c490547f84&TItems=2$-1$04-14-2023 07:51$05-14-2023 07:51$&DLang=zh-cn&CurLabel=CNY&AllocId=20a2fe6f954691282a4cee9b830052150e44c50f40998e0c2ba4e2d61d89788db385650f9bfd8f92e4878d62dae5ddaf4dace96c2bee82d65914f7971ae130308e022a0f902a8e8968e8aa0d00f9a783cc52b03d91b054eb7e1b417dab2424c490547f84&DPN=1; ASP.NET_SessionId=e30fdizkfprom1cfbrdvwwdk; xsrf_token=CfDJ8Dkuqwv-0VhLoFfD8dw7lYx6f7Lsv8xFskjVM_U1JWaj3WaDzAf7hrnIF4iTbJ8nC3o4C2hUPMnu2-3TGj15YaZ8A9vQGHmKELlUjKZnUyCbZ7F7VfJEgPoqzbKpgfDSY-f5MZQnoddgl_bf9FeW0B4; Hm_lvt_42891b044a69b4c9ddd70b0457437b1e=1681357473,1681433490; __gpi=UID=00000bf411dc2f71:T=1681357461:RT=1681434442:S=ALNI_MaaYXChrGR3NkIW_QgLsRbP7giV9w; agoda.search.01=SHist=1$12333$8138$1$1$1$0$0$$|4$8631674$8138$1$1$1$0$0$$|4$400123$8138$1$1$1$0$0$$|4$71833$8138$1$1$1$0$0$$|1$18343$8138$1$1$2$0$0$$|4$295060$8138$1$1$2$0$0$$|4$36453355$8138$1$1$1$0$0$$|1$12333$8139$1$1$2$0$0$$|4$36801927$8139$1$1$2$0$0$$&H=8138|1$36453355$8631674$36453355$400123$71833$400123$71833$36453355$295060$36453355$295060$36453355$295060$36453355$295060|0$36453355$36801927; Hm_lpvt_42891b044a69b4c9ddd70b0457437b1e=1681436801; agoda.analytics=Id=6630908614405058122&Signature=6216496710928710977&Expiry=1681440414755; utag_main=v_id:018778b6be86000b19a72d85a21a0506f044e06700ac2$_sn:4$_se:24$_ss:0$_st:1681438613922$ses_id:1681433485985%3Bexp-session$_pn:19%3Bexp-session',
    }
    return headers


def get_id_headers():
    headers = {
        'Host': 'www.agoda.cn',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'agoda.user.03=UserId=595ba893-8bde-4793-8fb2-c9890227500a; agoda.prius=PriusID=0&PointsMaxTraffic=Agoda; ASP.NET_SessionId=fqgfd1suklsqugpbk5rvbgfi; agoda.firstclicks=-1||||2023-04-13T10:43:33||fqgfd1suklsqugpbk5rvbgfi||{"IsPaid":false,"gclid":"","Type":""}; agoda.lastclicks=-1||||2023-04-13T10:43:33||fqgfd1suklsqugpbk5rvbgfi||{"IsPaid":false,"gclid":"","Type":""}; agoda.landings=-1|||fqgfd1suklsqugpbk5rvbgfi|2023-04-13T10:43:33|False|19-----1|||fqgfd1suklsqugpbk5rvbgfi|2023-04-13T10:43:33|False|20-----1|||fqgfd1suklsqugpbk5rvbgfi|2023-04-13T10:43:33|False|99; agoda.attr.03=ATItems=-1$04-13-2023 10:43$; agoda.price.01=PriceView=2; agoda.consent=CN||2023-04-13 10:43:35Z; agoda.familyMode=Mode=0; _ab50group=GroupB; _40-40-20Split=Group40B; __gads=ID=e1e93c9eb505cddb:T=1681357461:S=ALNI_MZpUEaxxnr7xp3VE1_MYAkoAo9nFg; __gpi=UID=00000bf411dc2f71:T=1681357461:RT=1681357461:S=ALNI_MaaYXChrGR3NkIW_QgLsRbP7giV9w; Hm_lvt_42891b044a69b4c9ddd70b0457437b1e=1681357473; agoda.vuser=UserId=e5a4065e-9af1-4121-8c40-e3835a570242; UserSession=595ba893-8bde-4793-8fb2-c9890227500a; agoda.version.03=CookieId=b054eb7e-1b41-47da-bb24-24c490547f84&TItems=2$-1$04-13-2023 10:43$05-13-2023 10:43$&DLang=zh-cn&CurLabel=CNY&AllocId=20a2fe6f954691282a4cee9b830052150e44c50f40998e0c2ba4e2d61d89788db385650f9bfd8f92e4878d62dae5ddaf4dace96c2bee82d65914f7971ae130308e022a0f902a8e8968e8aa0d00f9a783cc52b03d91b054eb7e1b417dab2424c490547f84&DPN=1; session_cache={"Cache":"hkdata1","Time":"638169595237565820","SessionID":"fqgfd1suklsqugpbk5rvbgfi","CheckID":"25ceedc33bf070ddc28a9ea889bb5fb382f32613","CType":"N"}; xsrf_token=CfDJ8Dkuqwv-0VhLoFfD8dw7lYxRULoZ8wqtxE1queup6f79QyxZ6Bp0W6cA2y2TilnBVnD1-fMZgxgi3BaRxYIoPeeJ5gS9qnI9ltUu7es2HW4BMVFE-i7v0nxvAHrjrNjf5QCvhrUYwc6mDPct_LL9WJU; agoda.search.01=SHist=1$12333$8138$1$1$1$0$0$$|4$8631674$8138$1$1$1$0$0$$|4$400123$8138$1$1$1$0$0$$|4$71833$8138$1$1$1$0$0$$|4$36453355$8138$1$1$1$0$0$$&H=8137|0$36453355$8631674$36453355$400123$71833$400123$71833$36453355; utag_main=v_id:018778b6be86000b19a72d85a21a0506f044e06700ac2$_sn:2$_se:21$_ss:0$_st:1681367355756$ses_id:1681363679644%3Bexp-session$_pn:19%3Bexp-session; Hm_lpvt_42891b044a69b4c9ddd70b0457437b1e=1681365559; agoda.analytics=Id=3682772621968845759&Signature=7092263486568603518&Expiry=1681369225266',
         }
    return headers


if __name__ == '__main__':
    crawl_run()
