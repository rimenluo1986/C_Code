# -*- coding:utf-8 -*-

import requests
import json
import datetime


def crawl():

    checkIn = datetime.date.today()                                     # 当天日期
    hotel_id = '70277'
    print('酒店id:：{}, 查询日期：{}'.format(hotel_id, checkIn))

    url = 'https://www.agoda.cn/api/cronos/property/BelowFoldParams/GetSecondaryData?finalPriceView=2&cid=-1&numberOfBedrooms=&familyMode=false&adults=1&children=0&rooms=1&maxRooms=0&checkIn={}&numberOfGuest=0&travellerType=-1&tspTypes=16&los=1&hotel_id={}&price_view=2&pagetypeid=7'.format(checkIn, hotel_id)
    req = requests.get(url, headers=headers())
    req_json = json.loads(req.text)

    all_prices_dict = {}
    all_prices_list = []

    for i in req_json['roomGridData']['masterRooms']:
        aa_dict = {}
        aa_list = []
        room_name = i['name']                                            # 房间名称
        type_alt_name = i['hotelRoomTypeAlternatName']
        room_id = i['id']  # 房间id

        try:
            beds = i['bedConfigurationSummary']['title']                 # 床型
        except:
            beds = ''

        aa_dict['room_id'] = room_id
        aa_dict['room_name'] = room_name
        aa_dict['room_type_alt_name'] = type_alt_name
        aa_dict['beds'] = beds

        for k in i['rooms']:
            prices_dict = {}
            type = [n['name'] for n in k['filters']]
            price = round(k['price']['display'])
            prices_dict['type'] = type
            prices_dict['price'] = price
            aa_list.append(prices_dict)
        aa_dict['rooms_groups'] = aa_list
        all_prices_list.append(aa_dict)
    all_prices_dict['hotel_id'] = hotel_id
    all_prices_dict['rooms_prices'] = all_prices_list
    print(all_prices_dict)


def headers():
    headers = {
        'Host': 'www.agoda.cn',
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'AG-Language-Id': '8',
        'CR-Currency-Id': '15',
        'AG-Analytics-Session-Id': '-6164117942132671267',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'x-raw-url': '/zh-cn/beijing-marriott-hotel-northeast/hotel/beijing-cn.html?finalPriceView=2&isShowMobileAppPrice=false&cid=-1&numberOfBedrooms=&familyMode=false&adults=1&children=0&rooms=1&maxRooms=0&checkIn=2023-04-19&isCalendarCallout=false&childAges=&numberOfGuest=0&missingChildAges=false&travellerType=-1&showReviewSubmissionEntry=false&currencyCode=CNY&isFreeOccSearch=false&isCityHaveAsq=false&tspTypes=16&los=1&searchrequestid=2e0db2b0-b5d1-40cc-8e82-ef148b8e7ea7',
        'Content-type': 'application/json;charset=UTF-8',
        'AG-Language-Locale': 'zh-cn',
        'X-Requested-With': 'XMLHttpRequest',
        'x-referer': '',
        'AG-REQUEST-ATTEMPT': '1',
        'AG-Correlation-Id': '061374c2-2917-4a1f-bdc1-b75b0b994a2d',
        'CR-Currency-Code': 'CNY',
        'sec-ch-ua-platform': '"Windows"',
        'Accept': '*/*',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.agoda.cn/zh-cn/beijing-marriott-hotel-northeast/hotel/beijing-cn.html?finalPriceView=2&isShowMobileAppPrice=false&cid=-1&numberOfBedrooms=&familyMode=false&adults=1&children=0&rooms=1&maxRooms=0&checkIn=2023-04-19&isCalendarCallout=false&childAges=&numberOfGuest=0&missingChildAges=false&travellerType=-1&showReviewSubmissionEntry=false&currencyCode=CNY&isFreeOccSearch=false&isCityHaveAsq=false&tspTypes=16&los=1&searchrequestid=2e0db2b0-b5d1-40cc-8e82-ef148b8e7ea7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'agoda.user.03=UserId=595ba893-8bde-4793-8fb2-c9890227500a; agoda.prius=PriusID=0&PointsMaxTraffic=Agoda; agoda.price.01=PriceView=2; agoda.consent=CN||2023-04-13 10:43:35Z; agoda.familyMode=Mode=0; _ab50group=GroupB; _40-40-20Split=Group40B; __gads=ID=e1e93c9eb505cddb:T=1681357461:S=ALNI_MZpUEaxxnr7xp3VE1_MYAkoAo9nFg; agoda.vuser=UserId=e5a4065e-9af1-4121-8c40-e3835a570242; deviceId=b14fbe39-b781-4397-b06b-dad27b9a9580; _ga=GA1.2.674414891.1681367313; agoda.firstclicks=-1||||2023-04-17T09:54:45||otzwkk23dmjxyy3kz4jeymwc||{"IsPaid":false,"gclid":"","Type":""}; agoda.lastclicks=-1||||2023-04-17T09:54:45||otzwkk23dmjxyy3kz4jeymwc||{"IsPaid":false,"gclid":"","Type":""}; agoda.landings=-1|||otzwkk23dmjxyy3kz4jeymwc|2023-04-17T09:54:45|False|19-----1|||otzwkk23dmjxyy3kz4jeymwc|2023-04-17T09:54:45|False|20-----1|||otzwkk23dmjxyy3kz4jeymwc|2023-04-17T09:54:45|False|99; agoda.attr.03=ATItems=-1$04-17-2023 09:54$; ASP.NET_SessionId=otzwkk23dmjxyy3kz4jeymwc; Hm_lvt_42891b044a69b4c9ddd70b0457437b1e=1681357473,1681433490,1681692803,1681700090; session_cache={"Cache":"hkdata1","Time":"638173065694049131","SessionID":"otzwkk23dmjxyy3kz4jeymwc","CheckID":"9e192f57d039d2cdd1548870bd298912ed8281f6","CType":"N"}; UserSession=595ba893-8bde-4793-8fb2-c9890227500a; agoda.version.03=CookieId=b054eb7e-1b41-47da-bb24-24c490547f84&TItems=2$-1$04-17-2023 09:54$05-17-2023 09:54$&DLang=zh-cn&CurLabel=CNY&AllocId=20a2fe6f954691282a4cee9b830052150e44c50f40998e0c2ba4e2d61d89788db385650f9bfd8f92e4878d62dae5ddaf4dace96c2bee82d65914f7971ae130308e022a0f902a8e8968e8aa0d00f9a783cc52b03d91b054eb7e1b417dab2424c490547f84&DPN=1&Alloc=&FEBuildVersion=; xsrf_token=CfDJ8Dkuqwv-0VhLoFfD8dw7lYy2iWTOBjrVzYSzyggTbEv38kGeJlqFD-OM3PJFXkcUpnOjJ3HPSjUFL-p0PAMrhvAlle84_0AmXBYKlggG4mPHkXeY1ETd0ceQX07fIMEnKwg9tM8oxiBzE2jNZbBR8JY; __gpi=UID=00000bf411dc2f71:T=1681357461:RT=1681891776:S=ALNI_MaaYXChrGR3NkIW_QgLsRbP7giV9w; _gid=GA1.2.1302809399.1681891946; agoda.search.01=SHist=4$36801927$8170$1$1$1$0$0$$|4$449$8150$1$1$2$0$0$$|4$16139534$8150$1$1$2$0$0$$|4$10004761$8150$1$1$2$0$0$$|4$34655199$8151$1$1$1$0$0$$|4$45468$8151$1$1$1$0$0$$|4$45468$8143$2$1$2$0$0$$|4$449$8143$1$1$2$0$0$$|4$45468$8143$1$1$1$0$0$$|4$45468$8143$1$1$2$0$0$$|4$161638$8143$1$1$2$0$0$$|4$161638$8143$1$1$1$0$0$$&H=8143|6$36453355$295060$36453355$295060$36453355$295060|5$36453355$36801927$289092$36801927|2$36801927$449$36801927$449$16139534$10004761$10002886$161638$1$161638$1$161638|1$36453355$34655199$45468|0$16716336$45468$449$45468$161638$45468$161638; Hm_lpvt_42891b044a69b4c9ddd70b0457437b1e=1681899039; agoda.analytics=Id=-6164117942132671267&Signature=5213036450511543919&Expiry=1681902723558; utag_main=v_id:018778b6be86000b19a72d85a21a0506f044e06700ac2$_sn:16$_se:29$_ss:0$_st:1681900924604$ses_id:1681895759515%3Bexp-session$_pn:23%3Bexp-session',
    }
    return headers


if __name__ == '__main__':
    crawl()
