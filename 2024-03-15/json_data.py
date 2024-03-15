import json

javascript_data = '<script>window[\'dataLayer\'] = window[\'dataLayer\'] || [];window[\'dataLayer\'].push({"experiments":{"CompactNavBar":"original","SearchHistoryDesktop":"original","SearchHistoryCompact":"filters_card","WhatsappLeadPhoneVerification":"original","EmailAlertsBanners":"original","SearchPageListingCard":"original","SearchHistory":"original","SearchHistoryMobile":"original","MobileListingCard":"swipeable_gallery","PropertyGalleryPhotoView":"fullscreen"},"device_type":"d","app_agent_version":"1.0","app_agent":"web","app_environment":"production","language":"en","area_unit":"Sq. Ft.","currency_unit":"AED","layout_preference":"list","url":"https:\\u002F\\u002Fwww.bayut.com\\u002Fproperty\\u002Fdetails-8724106.html","query_params":"?","route":{"routeName":"property","routeParams":{"externalID":"8724106"},"url":"\\u002Fproperty\\u002Fdetails-8724106.html","originalURL":"\\u002Fproperty\\u002Fdetails-8724106.html","referrer":null,"originalReferrer":null,"landingURL":"\\u002Fproperty\\u002Fdetails-8724106.html"},"referrer":null,"website_referrer":null,"starting_page_url":"https:\\u002F\\u002Fwww.bayut.com\\u002Fproperty\\u002Fdetails-8724106.html","anonymous_session_id":"6ea56f90-b1fe-4b69-a2f8-eb1ec1e2f6e6","user_profile":{"userStatus":"Anonymous"},"website_platform":"strat","ad_id":8724106,"object_id":8724106,"listingid":8724106,"listing_id":8724106,"property_badge":"truchecked","listing_state":"active","listing_type_name":"property","purpose":"Rent","purpose_id":2,"furnishing_status":"unfurnished","rent_frequency":"Yearly","ag":101483,"agent_id":101483,"agent_ids":101483,"agencyid":101483,"seller_type":"business","company_ids":101483,"seller_ids":2019916,"owner_user_id":2019916,"cat_id":16581,"locality":5785,"loc_breadcrumb":";5001;5002;5785;16581;","loc_city_id":";5002;","loc_city_name":";Dubai;","loc_id":";16581;","loc_name":";Park Terrace;","loc_neighbourhood_id":";5785;","loc_neighbourhood_name":";Arjan;","city_id":5785,"city_name":"Arjan","loc_1_id":";5002;","loc_1_name":";Dubai;","loc_2_id":";5785;","loc_2_name":";Arjan;","loc_3_id":";16581;","loc_3_name":";Park Terrace;","loc_4_id":"","loc_4_name":"","listing_type":4,"listing_type_id":4,"property_type":"apartments","category_1_id":1,"category_1_name":"residential","category_2_id":4,"category_2_name":"apartments","category_3_id":"","category_3_name":"","property_beds":1,"property_land_area":751.86,"property_price":73000,"property_baths_list":[2],"property_beds_list":[1],"property_area":69.8500796544,"latitude":25.063348145396,"longitude":55.234710520487,"reference_id":"Mohsin 1bhk 413","referrer_listing_id":null,"package_type":"superhot","package_hot":0,"package_free":0,"package_paid":0,"package_superhot":1,"is_active":"1","has_image":"1","number_of_photos":9,"number_of_videos":0,"listing_title":"Open View Very Spacious 1bhk Apartment Available With Balcony In Arjan Only 73k","condition":"any","content_pvtf":"9,0,0,0","price":73000,"listings":[{"agencyid":101483,"cat_id":16581,"has_image":"1","listing_type":4,"locality":5785,"package_type":"superhot","purpose":2,"listingid":8724106,"stat_type":"view"}],"page_screen_name":"detail","listing_pagetype":"offerdetail","page_type":"offerdetail","website_section":"Rent","page_section":"detail","dynx_itemid":8724106,"dynx_locid":16581,"dynx_category":4,"dynx_pagetype":"offerdetail"});</script>'

start_data= javascript_data.find('{')
end_data = javascript_data.rfind('}')
json_string = javascript_data[start_data:end_data+1]

json_data = json.loads(json_string)
# print(json.dumps(json_data, indent=4))

agencyid = json_data.get('agencyid', '')
print("agencyid:", agencyid)

price = json_data.get('price','')
print("price:",price)

currency_unit = json_data.get('currency_unit','')
print("currency_unit:",currency_unit)

purpose = json_data.get('purpose','')
print("purpose:",purpose)

listing_title = json_data.get('listing_title','')
print("listing_title:",purpose)

website_section = json_data.get('website_section','')
print("website_section:",website_section)

reference_id = json_data.get('reference_id','')
print("reference_id:",reference_id)

latitude = json_data.get('latitude','')
print("latitude:",latitude)

longitude = json_data.get('longitude','')
print("longitude:",longitude)

property_beds = json_data.get('property_beds','')
print("beds:",property_beds)

furnishing_status = json_data.get('furnishing_status','')
print("furnishing_status:",furnishing_status)