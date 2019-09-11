from __future__ import absolute_import, division, print_function, \
    with_statement
import requests
from peewee import * 
import json
import numpy
import logging
from multiprocessing.dummy import Pool as ThreadPool
import threading


objects_done = 0
objects_total = 0


def parse_json_save_to_sqlite(json_string, coords, id):
    post = id[0]
    hnr = id[1]
    long = coords["x"]
    lat = coords["y"]

    price_2015 = 0
    price_2016 = 0
    price_2017 = 0
    price_2018 = 0

    json_obj = None

    try:
        json_obj = json.loads(json_string)
    except Exception as e:
        logging.error(e)
        logging.error("there is some problem with json loading")

    building = {
        "house_number" : hnr,
        "postcode" : post,
        "longitude": long,
        "latitude": lat
    }
    for feature in json_obj["features"]:
        prop = feature["properties"]
        if "2015" in prop["wobj_wrd_peildatum"]:
            price_2015 = prop["wobj_wrd_woz_waarde"]
        if "2016" in prop["wobj_wrd_peildatum"]:
            price_2016 = prop["wobj_wrd_woz_waarde"]
        if "2017" in prop["wobj_wrd_peildatum"]:
            price_2017 = prop["wobj_wrd_woz_waarde"]
        if "2018" in prop["wobj_wrd_peildatum"]:
            price_2018 = prop["wobj_wrd_woz_waarde"]


    try:
        with open('./exported_data.csv', 'a+') as export:
            export.write(str(hnr) + "," + str(post) + "," + str(long) + "," + str(lat)+
                         str(price_2015) + "," + str(price_2016) + "," +
                         str(price_2017) + "," + str(price_2018) + ","+"\n")
    except:
        print("There was an error with saving to db.")



class BaseModel(Model):
    class Meta:
        database = SqliteDatabase("netherland_properties.db")
class PropertyModel(BaseModel):
    house_number = CharField(null=True)
    postcode = CharField(null=True)
    
    price_2015 = CharField(null=True)
    price_2016 = CharField(null=True)
    price_2017 = CharField(null=True)
    price_2018 = CharField(null=True)

    longitude = CharField(null=True)
    latitude = CharField(null=True)

def init_database():
    db = SqliteDatabase("netherland_properties.db")
    db.connect()
    try:
        db.drop_tables([PropertyModel])
    except:
        pass
    db.create_tables([PropertyModel])
    db.close()


def scrape_obj_from_id_to_id(f=None):
    if f is None:
        raise Exception("From id is None, check the script please. ")

    # format from_id
    from_id = '%012d'%f
    
    # initiate a request object
    s = requests.Session()
    try:
        s.get("https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&")
    except Exception as e:
        logging.error(e)
        logging.error("Please fix your network connection status immediately")
        logging.error("the script will wait for you about 10 seconds :< ")

    xml_obj = \
    """
    <wfs:GetFeature xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0"
                    xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <wfs:Query typeName="wozloket:woz_woz_object" srsName="EPSG:28992" xmlns:WozViewer="http://WozViewer.geonovum.nl"
                   xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                <ogc:And>
                    <ogc:PropertyIsEqualTo matchCase="true">
                        <ogc:PropertyName>wobj_bag_obj_id</ogc:PropertyName>
                        <ogc:Literal>%s</ogc:Literal>
                    </ogc:PropertyIsEqualTo>
                </ogc:And>
            </ogc:Filter>
        </wfs:Query>
    </wfs:GetFeature>
    """
    xml_obj = xml_obj%(str(from_id),)
    
    response = None
    try:
        response = s.post(url="https://www.wozwaardeloket.nl/woz-proxy/wozloket", data=xml_obj)
        # print(response.json())
        print("scraping woz obj with id "+str(from_id)+". \n")
    except Exception as e:
        logging.error(e)
        logging.error("request has met problem. ")
        logging.error("from_id=%s"%str(from_id))

    text_response = None
    try:
        text_response = response.text
    except:
        text_response = None
        
    return text_response


def scrape_range_and_save(array_with_post_and_hnr):
    global objects_total, objects_done
    post = array_with_post_and_hnr[0]
    hnr = array_with_post_and_hnr[1]

    r = requests.get('https://www.wozwaardeloket.nl/api/geocoder/v3/suggest?query='+post+"%20"+hnr)
    id = r.json()["docs"][0]["id"]
    r = requests.get('https://www.wozwaardeloket.nl/api/geocoder/v3/lookup?id='+id)
    r_json = r.json()
    bag_obj_id = r_json['adresseerbaarobject_id']
    coords = r_json['centroide_ll']
    json_string = scrape_obj_from_id_to_id(int(bag_obj_id))
    parse_json_save_to_sqlite(json_string=json_string, coords=coords, id=array_with_post_and_hnr)
    objects_done += 1
    print("----------------- Process: {:2.2f}%------------------".format((objects_done)*100/(objects_total)))


def stage1_scrape_all_obj(array_with_post_and_house):
    global objects_total, objects_done

    # threads = input("how many threads you want to use? (e.g. 100)")

    print("deploying threads, please wait .... ")
    total_steps = range(len(array_with_post_and_house))
    objects_total = len(total_steps)

    #pool = ThreadPool(int(threads))

    for entry in array_with_post_and_house:
        scrape_range_and_save(entry)
        # pool.map(scrape_range_and_save, entry)
        # pool.close()
        # pool.join()

    objects_total = 0
    objects_done = 0


def scrape_each_property_price(property_id):
    s = requests.Session()

    try:
        s.get("https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&")
    except Exception as e:
        logging.error(e)
        logging.error("Please fix your network connection status immediately")
        logging.error("the script will wait for you about 10 seconds :< ")

    xml_obj = \
    """
    <wfs:GetFeature
        xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0" xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <wfs:Query typeName="wozloket:woz_woz_object" srsName="EPSG:28992"
            xmlns:WozViewer="http://WozViewer.geonovum.nl"
            xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:Filter
                xmlns:ogc="http://www.opengis.net/ogc">
                <ogc:PropertyIsEqualTo matchCase="true">
                    <ogc:PropertyName>wobj_obj_id</ogc:PropertyName>
                    <ogc:Literal>%s</ogc:Literal>
                </ogc:PropertyIsEqualTo>
            </ogc:Filter>
        </wfs:Query>
    </wfs:GetFeature>
    """
    
    xml_obj = xml_obj%str(property_id)
    
    response = None
    try:
        response = s.post(url="https://www.wozwaardeloket.nl/woz-proxy/wozloket", data=xml_obj)
        print("--------scraping price of property id %s-------"%str(property_id), end="\n")
    except Exception as e:
        logging.error(e)
        logging.error("request has met problem. ")
        logging.error("property_id: %s"%str(property_id))
    
    t_response = None
    try:
    	t_response = response.text
    except:
    	t_response = None

    return t_response


def parse_each_property_price(json_string):
    json_objs = []
    try:
        json_objs = json.loads(json_string)['features']
    except Exception as e:
        logging.error(e)
        logging.error("there is some problem with json loading")

    price15, price16, price17 = None, None, None
    if json_objs is not None:
        for obj in json_objs:
            each_obj = None
            try:
                each_obj = obj['properties']
            except Exception as e:
                logging.error(e)
            
            if '2015' in str(each_obj['wobj_wrd_peildatum']):
                try:
                    price15 = int(each_obj['wobj_wrd_woz_waarde'])/1000
                    price15 = "{:.3f}".format(price15)
                except Exception as e:
                    price15 = None
                try:
                    price16 = int(each_obj['wobj_huidige_woz_waarde'])/1000
                    price16 = "{:.3f}".format(price16)
                except Exception as e:
                    price16 = None
            elif '2016' in str(each_obj['wobj_wrd_peildatum']):
                try:
                    price17 = int(each_obj['wobj_huidige_woz_waarde'])/1000
                    price17 = "{:.3f}".format(price17)
                except Exception as e:
                    price17 = None

    return price15, price16, price17


if __name__ == '__main__':
    init_database()
    stage1_scrape_all_obj([
                            ["5623PC", "314"],
                            ["5623PC", "312"],
                            ["6214AT", "22"]
    ])
    print("script finished :) ")